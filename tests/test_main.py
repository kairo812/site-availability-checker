# -*- coding: utf-8 -*-
"""
Автотесты приложения. Используется только стандартный unittest.

Интернет для тестов не нужен: сайты «изображает» локальный сервер aiohttp,
который умеет отвечать кодами 200, 403, 404, 405, 500, делать редирект и
«зависать» (для проверки таймаута).

Запуск из корня проекта:
    python -m unittest discover -s tests -v
"""

import asyncio
import csv
import io
import json
import socket
import sys
import tempfile
import unittest
from pathlib import Path
from typing import List

from aiohttp import web
from rich.console import Console

# Чтобы «import main» работал при запуске из любой папки
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main as app_module  # noqa: E402
from main import App, DataManager, SiteChecker  # noqa: E402


# ---------------------------------------------------------------------
#                    Локальный тестовый сервер
# ---------------------------------------------------------------------
class LocalServer:
    """Маленький HTTP-сервер с набором «сайтов» на разные случаи жизни."""

    def __init__(self) -> None:
        self.in_flight: int = 0       # сколько запросов обрабатывается прямо сейчас
        self.max_in_flight: int = 0   # максимум, который наблюдался
        self.methods_seen: List[str] = []
        self.runner: web.AppRunner
        self.base: str = ""

    async def start(self) -> None:
        app = web.Application()

        async def ok(request: web.Request) -> web.Response:
            return web.Response(text="ok")

        async def forbidden(request: web.Request) -> web.Response:
            return web.Response(status=403)

        async def missing(request: web.Request) -> web.Response:
            return web.Response(status=404)

        async def boom(request: web.Request) -> web.Response:
            return web.Response(status=500)

        async def only_get(request: web.Request) -> web.Response:
            self.methods_seen.append(request.method)
            return web.Response(text="только GET")

        async def redirect(request: web.Request) -> web.Response:
            raise web.HTTPFound("/ok")

        async def slow(request: web.Request) -> web.Response:
            await asyncio.sleep(3)
            return web.Response(text="поздно")

        async def gauge(request: web.Request) -> web.Response:
            # Считаем, сколько запросов выполняется одновременно
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            await asyncio.sleep(0.15)
            self.in_flight -= 1
            return web.Response(text="ok")

        app.router.add_get("/ok", ok)
        app.router.add_get("/forbidden", forbidden)
        app.router.add_get("/missing", missing)
        app.router.add_get("/boom", boom)
        # allow_head=False -> на HEAD сервер ответит 405, как некоторые настоящие сайты
        app.router.add_get("/only-get", only_get, allow_head=False)
        app.router.add_get("/redirect", redirect)
        app.router.add_get("/slow", slow)
        app.router.add_get("/gauge", gauge)

        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)  # порт 0 - любой свободный
        await site.start()
        port: int = self.runner.addresses[0][1]
        self.base = f"http://127.0.0.1:{port}"

    async def stop(self) -> None:
        await self.runner.cleanup()

    def url(self, path: str) -> str:
        return self.base + path


def free_closed_port() -> int:
    """Возвращает номер порта, на котором точно никто не слушает."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def make_record(url: str, available: bool = True, ms: float = 100.0,
                category: str = "ok", stamp: str = "2026-01-01 10:00:00") -> dict:
    """Быстро собирает запись истории для тестов DataManager."""
    return {
        "url": url, "final_url": url, "redirects": 0, "available": available,
        "category": category, "status_code": 200 if available else None,
        "method": "HEAD", "response_time_ms": ms if available else None,
        "error": None if available else "Таймаут", "checked_at": stamp,
    }


# ---------------------------------------------------------------------
#                   1. Проверка и разбор адресов
# ---------------------------------------------------------------------
class TestUrlParsing(unittest.TestCase):
    """Сценарии: корректный и некорректный ввод адресов."""

    def test_adds_https_when_scheme_missing(self) -> None:
        self.assertEqual(SiteChecker.normalize_url("google.com"), "https://google.com")

    def test_keeps_http_path_and_port(self) -> None:
        self.assertEqual(SiteChecker.normalize_url("http://Example.com:8080/a?b=1"),
                         "http://example.com:8080/a?b=1")

    def test_cyrillic_domain_converted_to_punycode(self) -> None:
        self.assertEqual(SiteChecker.normalize_url("пример.рф"),
                         "https://xn--e1afmkfd.xn--p1ai")

    def test_localhost_is_allowed(self) -> None:
        self.assertEqual(SiteChecker.normalize_url("localhost:8000"), "https://localhost:8000")

    def test_invalid_input_is_rejected(self) -> None:
        bad_values = ["", "   ", "not a url", "google", "ftp://example.com", "http://",
                      "a..b.com", "javascript:alert(1)", "example.com:99999",
                      "user:pass@example.com", "exa_mple.com", "-bad-.com"]
        for value in bad_values:
            with self.subTest(value=value):
                self.assertIsNone(SiteChecker.normalize_url(value))

    def test_parse_urls_splits_dedups_and_reports_invalid(self) -> None:
        urls, invalid = SiteChecker.parse_urls("google.com, github.com;google.com  мусор\nhttps://python.org")
        self.assertEqual(urls, ["https://google.com", "https://github.com", "https://python.org"])
        self.assertEqual(invalid, ["мусор"])

    def test_classify_status_codes(self) -> None:
        expected = {200: "ok", 301: "ok", 401: "restricted", 403: "restricted",
                    429: "restricted", 404: "client_error", 410: "client_error",
                    500: "server_error", 503: "server_error", None: "unreachable"}
        for code, category in expected.items():
            with self.subTest(code=code):
                self.assertEqual(SiteChecker.classify(code), category)


# ---------------------------------------------------------------------
#                 2. Асинхронная проверка сайтов
# ---------------------------------------------------------------------
class TestSiteChecker(unittest.IsolatedAsyncioTestCase):
    """Проверка реальных HTTP-запросов к локальному серверу."""

    async def asyncSetUp(self) -> None:
        self.server = LocalServer()
        await self.server.start()
        self.checker = SiteChecker(timeout=1, max_concurrent=10, retries=0)

    async def asyncTearDown(self) -> None:
        await self.server.stop()

    async def check(self, path: str):
        results = await self.checker.check_many([self.server.url(path)])
        return results[0]

    async def test_available_site_uses_head(self) -> None:
        result = await self.check("/ok")
        self.assertTrue(result["available"])
        self.assertEqual(result["status_code"], 200)
        self.assertEqual(result["method"], "HEAD")
        self.assertIsNotNone(result["response_time_ms"])

    async def test_405_triggers_get_fallback(self) -> None:
        result = await self.check("/only-get")
        self.assertTrue(result["available"])
        self.assertEqual(result["method"], "GET")      # перешли на GET
        self.assertEqual(result["status_code"], 200)
        self.assertIn("GET", self.server.methods_seen)

    async def test_404_is_not_available(self) -> None:
        result = await self.check("/missing")
        self.assertFalse(result["available"])
        self.assertEqual(result["category"], "client_error")

    async def test_500_is_server_error(self) -> None:
        result = await self.check("/boom")
        self.assertFalse(result["available"])
        self.assertEqual(result["category"], "server_error")

    async def test_403_means_alive_but_restricted(self) -> None:
        result = await self.check("/forbidden")
        self.assertTrue(result["available"])
        self.assertEqual(result["category"], "restricted")

    async def test_redirect_is_followed(self) -> None:
        result = await self.check("/redirect")
        self.assertTrue(result["available"])
        self.assertEqual(result["redirects"], 1)
        self.assertTrue(result["final_url"].endswith("/ok"))

    async def test_timeout_does_not_crash(self) -> None:
        result = await self.check("/slow")  # сервер молчит 3 с, таймаут 1 с
        self.assertFalse(result["available"])
        self.assertIsNone(result["status_code"])
        self.assertIn("Таймаут", result["error"])

    async def test_connection_refused_does_not_crash(self) -> None:
        url = f"http://127.0.0.1:{free_closed_port()}/"
        result = (await self.checker.check_many([url]))[0]
        self.assertFalse(result["available"])
        self.assertEqual(result["category"], "unreachable")
        self.assertTrue(result["error"])

    async def test_semaphore_limits_parallel_requests(self) -> None:
        checker = SiteChecker(timeout=5, max_concurrent=3, retries=0)
        urls = [self.server.url(f"/gauge?i={i}") for i in range(15)]
        results = await checker.check_many(urls)
        self.assertEqual(len(results), 15)
        self.assertTrue(all(r["available"] for r in results))
        self.assertLessEqual(self.server.max_in_flight, 3)   # лимит соблюдён
        self.assertGreaterEqual(self.server.max_in_flight, 2)  # но параллелизм был

    async def test_concurrency_never_exceeds_ten(self) -> None:
        self.assertEqual(SiteChecker(max_concurrent=500).max_concurrent, 10)
        self.assertEqual(SiteChecker(max_concurrent=0).max_concurrent, 1)

    async def test_results_keep_input_order(self) -> None:
        urls = [self.server.url("/missing"), self.server.url("/ok"), self.server.url("/boom")]
        results = await self.checker.check_many(urls)
        self.assertEqual([r["url"] for r in results], urls)

    async def test_progress_callback_called_for_each_site(self) -> None:
        done: List[str] = []
        urls = [self.server.url("/ok"), self.server.url("/missing")]
        await self.checker.check_many(urls, lambda r: done.append(r["url"]))
        self.assertEqual(sorted(done), sorted(urls))

    async def test_empty_list_returns_empty(self) -> None:
        self.assertEqual(await self.checker.check_many([]), [])


# ---------------------------------------------------------------------
#                    3. Хранение данных и статистика
# ---------------------------------------------------------------------
class TestDataManager(unittest.TestCase):
    """Сценарии: сохранение, повторная загрузка, поиск, фильтры, экспорт."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.hist = self.dir / "history.json"
        self.cfg = self.dir / "config.json"
        self.dm = DataManager(self.hist, self.cfg)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_save_and_reload(self) -> None:
        self.dm.add_records([make_record("https://a.com"), make_record("https://b.com")])
        reloaded = DataManager(self.hist, self.cfg)
        self.assertEqual([r["url"] for r in reloaded.history], ["https://a.com", "https://b.com"])
        self.assertFalse((self.dir / "history.json.tmp").exists())  # временного файла не осталось

    def test_file_has_schema_version(self) -> None:
        self.dm.add_records([make_record("https://a.com")])
        data = json.loads(self.hist.read_text(encoding="utf-8"))
        self.assertEqual(data["version"], app_module.SCHEMA_VERSION)

    def test_corrupted_file_is_backed_up_not_lost(self) -> None:
        self.hist.write_text("{ это не json", encoding="utf-8")
        dm = DataManager(self.hist, self.cfg)
        self.assertEqual(dm.history, [])
        self.assertIsNotNone(dm.load_warning)
        self.assertTrue((self.dir / "history.json.bak").exists())

    def test_legacy_list_format_is_supported(self) -> None:
        self.hist.write_text(json.dumps([make_record("https://old.com")]), encoding="utf-8")
        self.assertEqual(len(DataManager(self.hist, self.cfg).history), 1)

    def test_history_is_trimmed(self) -> None:
        old_limit = app_module.MAX_HISTORY_ITEMS
        app_module.MAX_HISTORY_ITEMS = 5
        try:
            self.dm.add_records([make_record(f"https://s{i}.com") for i in range(8)])
        finally:
            app_module.MAX_HISTORY_ITEMS = old_limit
        self.assertEqual(len(self.dm.history), 5)
        self.assertEqual(self.dm.history[-1]["url"], "https://s7.com")

    def test_search_filter_and_sort(self) -> None:
        self.dm.add_records([
            make_record("https://alpha.com", True, 500, stamp="2026-01-01 10:00:00"),
            make_record("https://beta.org", False, 0, "unreachable", "2026-01-01 10:00:01"),
            make_record("https://gamma.com", True, 50, stamp="2026-01-01 10:00:02"),
        ])
        self.assertEqual([r["url"] for r in self.dm.query(text="COM")],
                         ["https://gamma.com", "https://alpha.com"])        # поиск без регистра
        self.assertEqual([r["url"] for r in self.dm.query(only="unavailable")],
                         ["https://beta.org"])                              # фильтр
        self.assertEqual([r["url"] for r in self.dm.query(sort="fastest")][:2],
                         ["https://gamma.com", "https://alpha.com"])        # сортировка по времени
        self.assertEqual(self.dm.query(sort="slowest")[-1]["url"], "https://beta.org")  # без времени - в конец
        self.assertEqual(self.dm.query(sort="url")[0]["url"], "https://alpha.com")
        self.assertEqual(self.dm.query(sort="oldest")[0]["url"], "https://alpha.com")
        self.assertEqual(self.dm.query(text="нет такого"), [])

    def test_statistics(self) -> None:
        self.assertEqual(self.dm.get_statistics()["total"], 0)  # пустая история - не падаем
        self.dm.add_records([make_record("https://a.com", True, 100),
                             make_record("https://b.com", True, 300),
                             make_record("https://c.com", False, 0, "unreachable")])
        stats = self.dm.get_statistics()
        self.assertEqual((stats["total"], stats["available"], stats["unavailable"]), (3, 2, 1))
        self.assertAlmostEqual(stats["uptime_percent"], 66.666, places=1)
        self.assertEqual(stats["avg_time_ms"], 200)

    def test_per_site_stats(self) -> None:
        self.dm.add_records([make_record("https://a.com"), make_record("https://a.com", False, 0, "unreachable"),
                             make_record("https://b.com")])
        rows = {r["url"]: r for r in self.dm.per_site_stats()}
        self.assertEqual(rows["https://a.com"]["checks"], 2)
        self.assertEqual(rows["https://a.com"]["uptime_percent"], 50)
        self.assertEqual(self.dm.per_site_stats()[0]["url"], "https://a.com")  # проблемные первыми

    def test_export_csv_readable_with_cyrillic(self) -> None:
        self.dm.add_records([make_record("https://a.com"), make_record("https://b.com", False, 0, "unreachable")])
        path = self.dm.export_csv(self.dir / "out" / "report.csv")
        with open(path, encoding="utf-8-sig", newline="") as file:
            rows = list(csv.DictReader(file, delimiter=";"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["available"], "да")
        self.assertEqual(rows[1]["available"], "нет")
        self.assertEqual(rows[1]["error"], "Таймаут")

    def test_clear(self) -> None:
        self.dm.add_records([make_record("https://a.com")])
        self.dm.clear()
        self.assertEqual(DataManager(self.hist, self.cfg).history, [])

    def test_settings_saved_validated_and_reset(self) -> None:
        self.dm.update_settings(timeout=25, concurrency=4)
        reloaded = DataManager(self.hist, self.cfg)
        self.assertEqual((reloaded.settings["timeout"], reloaded.settings["concurrency"]), (25, 4))
        self.dm.update_settings(concurrency=999)  # некорректное -> значение по умолчанию
        self.assertEqual(self.dm.settings["concurrency"], app_module.DEFAULT_SETTINGS["concurrency"])
        self.dm.reset_settings()
        self.assertEqual(self.dm.settings, app_module.DEFAULT_SETTINGS)

    def test_broken_config_falls_back_to_defaults(self) -> None:
        self.cfg.write_text("мусор", encoding="utf-8")
        self.assertEqual(DataManager(self.hist, self.cfg).settings, app_module.DEFAULT_SETTINGS)
        self.cfg.write_text(json.dumps({"timeout": "10", "concurrency": True, "retries": -5}), encoding="utf-8")
        self.assertEqual(DataManager(self.hist, self.cfg).settings, app_module.DEFAULT_SETTINGS)

    def test_read_sites_file_skips_comments(self) -> None:
        path = self.dir / "sites.txt"
        path.write_text("# заголовок\n\ngoogle.com   # поисковик\n  github.com\n# ещё комментарий\n",
                        encoding="utf-8")
        self.assertEqual(DataManager.read_sites_file(path), ["google.com", "github.com"])


# ---------------------------------------------------------------------
#          4. Интерфейс: пакетный режим (то же, что и из меню)
# ---------------------------------------------------------------------
class TestAppBatch(unittest.IsolatedAsyncioTestCase):
    """Проверяет App.run_batch - код возврата, сохранение и обработку ошибок ввода."""

    async def asyncSetUp(self) -> None:
        self.server = LocalServer()
        await self.server.start()
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.buffer = io.StringIO()
        self.console = Console(file=self.buffer, width=130, force_terminal=False)
        self.dm = DataManager(self.dir / "history.json", self.dir / "config.json")
        self.dm.update_settings(timeout=2, retries=0)
        self.app = App(data=self.dm, console=self.console)

    async def asyncTearDown(self) -> None:
        await self.server.stop()
        self._tmp.cleanup()

    async def run_batch(self, *args, **kwargs) -> int:
        # run_batch внутри использует asyncio.run, поэтому запускаем его в отдельном потоке
        return await asyncio.to_thread(self.app.run_batch, *args, **kwargs)

    async def test_all_available_returns_0_and_saves_history(self) -> None:
        code = await self.run_batch([self.server.url("/ok"), self.server.url("/redirect")])
        self.assertEqual(code, 0)
        self.assertEqual(len(DataManager(self.dir / "history.json", self.dir / "config.json").history), 2)
        self.assertIn("ДОСТУПЕН", self.buffer.getvalue())

    async def test_some_unavailable_returns_1(self) -> None:
        code = await self.run_batch([self.server.url("/ok"), self.server.url("/missing")])
        self.assertEqual(code, 1)
        self.assertIn("НЕ НАЙДЕН", self.buffer.getvalue())

    async def test_no_save_flag(self) -> None:
        await self.run_batch([self.server.url("/ok")], save=False)
        self.assertEqual(self.dm.history, [])

    async def test_invalid_input_returns_2(self) -> None:
        self.assertEqual(await self.run_batch(["!!!", "не адрес"]), 2)

    async def test_missing_file_returns_2(self) -> None:
        self.assertEqual(await self.run_batch([], file=str(self.dir / "нет_файла.txt")), 2)

    async def test_file_with_sites(self) -> None:
        path = self.dir / "list.txt"
        path.write_text(f"# тест\n{self.server.url('/ok')}\n{self.server.url('/boom')} # сломан\n",
                        encoding="utf-8")
        self.assertEqual(await self.run_batch([], file=str(path)), 1)
        self.assertEqual(len(self.dm.history), 2)

    async def test_brackets_in_url_do_not_break_output(self) -> None:
        # Квадратные скобки в адресе не должны восприниматься как разметка rich
        code = await self.run_batch([self.server.url("/ok?a[]=1&b=[/bold]")])
        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
