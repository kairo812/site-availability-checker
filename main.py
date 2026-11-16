# -*- coding: utf-8 -*-
"""
Site Availability Checker - приложение для проверки доступности сайтов.

Курсовая работа по дисциплине «Основы алгоритмизации и программирования».
Тема: «Разработка приложения для проверки доступности сайтов».
Автор: Маматкулов Фирдавс, группа АСОИ-1-25, КОМТЕХНО.

Архитектура (три класса, у каждого одна зона ответственности):
    DataManager - всё, что связано с файлами: история (JSON), настройки,
                  экспорт в CSV, чтение списка сайтов, поиск/фильтры/статистика;
    SiteChecker - асинхронная проверка сайтов (aiohttp + asyncio), не знает
                  ни про файлы, ни про консоль;
    App         - интерфейс: меню, ввод с проверкой, красивый вывод (rich).

Запуск:
    python main.py                        - интерактивное меню
    python main.py google.com github.com  - быстрая проверка без меню
    python main.py -f sites.txt           - проверка списка из файла
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import socket
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlsplit, urlunsplit

import aiohttp
from rich import box
from rich.align import Align
from rich.console import Console, Group
from rich.markup import escape
from rich.panel import Panel
from rich.progress import (BarColumn, MofNCompleteColumn, Progress,
                           SpinnerColumn, TextColumn, TimeElapsedColumn)
from rich.table import Table
from rich.text import Text

# Псевдоним типа: результат проверки одного сайта - обычный словарь
CheckResult = Dict[str, Any]

# =====================================================================
#                              КОНСТАНТЫ
# =====================================================================
VERSION: str = "1.0.0"

# Все файлы лежат рядом со скриптом, а не в «текущей папке терминала»
BASE_DIR: Path = Path(__file__).resolve().parent
HISTORY_FILE: Path = BASE_DIR / "history.json"
CONFIG_FILE: Path = BASE_DIR / "config.json"
EXPORT_DIR: Path = BASE_DIR / "exports"
DEFAULT_SITES_FILE: Path = BASE_DIR / "sites.txt"
LOG_FILE: Path = BASE_DIR / "site_checker.log"

SCHEMA_VERSION: int = 1          # Версия формата history.json (для будущих изменений)
MAX_CONCURRENT_LIMIT: int = 10   # Жёсткий потолок одновременных запросов
MAX_HISTORY_ITEMS: int = 5000    # Сколько записей хранить в истории
HISTORY_PAGE_SIZE: int = 20      # Сколько строк истории показывать за раз
RETRY_DELAY: float = 0.5         # Пауза перед повторной попыткой, секунды
FALLBACK_STATUSES: Tuple[int, ...] = (405, 501)  # Коды, при которых HEAD -> GET
USER_AGENT: str = "Mozilla/5.0 (compatible; SiteAvailabilityChecker/1.0)"

# Настройки по умолчанию и допустимые границы (минимум, максимум)
DEFAULT_SETTINGS: Dict[str, int] = {"timeout": 10, "concurrency": 10, "retries": 1}
SETTINGS_LIMITS: Dict[str, Tuple[int, int]] = {
    "timeout": (1, 60),
    "concurrency": (1, MAX_CONCURRENT_LIMIT),
    "retries": (0, 3),
}

# Категории результата проверки
CAT_OK: str = "ok"                    # 2xx / 3xx - сайт отвечает нормально
CAT_RESTRICTED: str = "restricted"    # 401 / 403 / 429 - жив, но не пускает бота
CAT_CLIENT_ERROR: str = "client_error"  # 4xx (404 и др.) - страницы нет
CAT_SERVER_ERROR: str = "server_error"  # 5xx - сервер сломался
CAT_UNREACHABLE: str = "unreachable"    # нет ответа: DNS, таймаут, SSL...

CATEGORY_TITLES: Dict[str, str] = {
    CAT_OK: "ДОСТУПЕН",
    CAT_RESTRICTED: "ОГРАНИЧЕН",
    CAT_CLIENT_ERROR: "НЕ НАЙДЕН",
    CAT_SERVER_ERROR: "ОШИБКА СЕРВЕРА",
    CAT_UNREACHABLE: "НЕДОСТУПЕН",
}
CATEGORY_STYLES: Dict[str, str] = {
    CAT_OK: "bold green",
    CAT_RESTRICTED: "bold yellow",
    CAT_CLIENT_ERROR: "bold red",
    CAT_SERVER_ERROR: "bold red",
    CAT_UNREACHABLE: "bold red",
}

# Поля, которые попадают в CSV-отчёт (в таком порядке)
CSV_FIELDS: List[str] = [
    "checked_at", "url", "available", "category", "status_code", "method",
    "response_time_ms", "redirects", "final_url", "error",
]

# Регулярное выражение для проверки имени хоста (после перевода в punycode)
_HOST_RE = re.compile(
    r"^(?=.{1,253}$)[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?"
    r"(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$"
)

# Логгер: технические подробности ошибок пишем в файл, а не в консоль
LOG = logging.getLogger("site_checker")


# =====================================================================
#                          КЛАСС DataManager
# =====================================================================
class DataManager:
    """Хранение и обработка данных: история, настройки, CSV, статистика."""

    def __init__(self, history_path: Path = HISTORY_FILE,
                 config_path: Path = CONFIG_FILE) -> None:
        """Запоминает пути к файлам и сразу загружает историю и настройки."""
        self.history_path: Path = Path(history_path)
        self.config_path: Path = Path(config_path)
        # Сообщение для пользователя, если файл истории пришлось «спасать»
        self.load_warning: Optional[str] = None
        self.history: List[CheckResult] = self._load_history()
        self.settings: Dict[str, int] = self._load_settings()

    # ------------------------- Работа с файлами -------------------------
    @staticmethod
    def _atomic_write(path: Path, payload: Any) -> bool:
        """
        Безопасная запись JSON: сначала во временный файл, потом замена.
        Если программа упадёт посреди записи, старый файл останется целым.
        """
        tmp_path: Path = path.with_name(path.name + ".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as file:
                # ensure_ascii=False - чтобы русский текст не превращался в \uXXXX
                json.dump(payload, file, ensure_ascii=False, indent=2)
                file.flush()
                os.fsync(file.fileno())
            os.replace(tmp_path, path)  # атомарная замена файла
            return True
        except OSError as error:
            LOG.error("Не удалось записать %s: %s", path, error)
            return False

    def _quarantine_history(self, reason: str) -> None:
        """Повреждённый файл не удаляем, а переименовываем в .bak."""
        backup: Path = self.history_path.with_name(self.history_path.name + ".bak")
        try:
            os.replace(self.history_path, backup)
            self.load_warning = (f"Файл истории повреждён ({reason}). Копия сохранена "
                                 f"как {backup.name}, начата новая история.")
        except OSError as error:
            LOG.error("Не удалось переименовать повреждённый файл: %s", error)
            self.load_warning = f"Файл истории повреждён ({reason}); начата новая история."

    def _load_history(self) -> List[CheckResult]:
        """Читает историю с диска. Любая проблема не должна ронять программу."""
        if not self.history_path.exists():
            return []  # первый запуск - истории ещё нет
        try:
            with open(self.history_path, "r", encoding="utf-8") as file:
                data: Any = json.load(file)
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._quarantine_history("не удалось разобрать JSON")
            return []
        except OSError as error:
            LOG.error("Не удалось прочитать историю: %s", error)
            self.load_warning = f"Не удалось прочитать историю: {error}"
            return []
        # Поддерживаем и новый формат {"version":..,"records":[..]}, и старый (список)
        records: Any = data.get("records") if isinstance(data, dict) else data
        if not isinstance(records, list):
            self._quarantine_history("неизвестный формат")
            return []
        return [r for r in records if isinstance(r, dict) and "url" in r]

    def _save_history(self) -> bool:
        """Сохраняет историю вместе с номером версии формата."""
        return self._atomic_write(
            self.history_path,
            {"version": SCHEMA_VERSION, "records": self.history},
        )

    # ---------------------------- История ----------------------------
    def add_records(self, records: List[CheckResult]) -> bool:
        """Добавляет результаты проверки в историю и сохраняет файл."""
        self.history.extend(records)
        # Храним только последние MAX_HISTORY_ITEMS записей, чтобы файл не рос вечно
        self.history = self.history[-MAX_HISTORY_ITEMS:]
        return self._save_history()

    def clear(self) -> bool:
        """Полностью очищает историю."""
        self.history = []
        return self._save_history()

    def query(self, text: str = "", only: str = "all",
              sort: str = "newest") -> List[CheckResult]:
        """
        Поиск, фильтрация и сортировка истории.
            text - подстрока в адресе сайта (поиск);
            only - 'all' | 'available' | 'unavailable' (фильтр);
            sort - 'newest' | 'oldest' | 'slowest' | 'fastest' | 'url' (сортировка).
        """
        rows: List[CheckResult] = list(self.history)

        # 1) Поиск по подстроке (без учёта регистра)
        needle: str = text.strip().lower()
        if needle:
            rows = [r for r in rows if needle in str(r.get("url", "")).lower()]

        # 2) Фильтр по доступности
        if only == "available":
            rows = [r for r in rows if r.get("available")]
        elif only == "unavailable":
            rows = [r for r in rows if not r.get("available")]

        # 3) Сортировка
        if sort in ("slowest", "fastest"):
            # Записи без времени (ошибки) всегда уходят в конец списка
            timed = [r for r in rows if r.get("response_time_ms") is not None]
            untimed = [r for r in rows if r.get("response_time_ms") is None]
            timed.sort(key=lambda r: r["response_time_ms"], reverse=(sort == "slowest"))
            return timed + untimed
        if sort == "url":
            rows.sort(key=lambda r: str(r.get("url", "")).lower())
            return rows
        rows.sort(key=lambda r: str(r.get("checked_at", "")), reverse=(sort == "newest"))
        return rows

    # --------------------------- Статистика ---------------------------
    def get_statistics(self) -> Dict[str, Any]:
        """Общая статистика по всей истории."""
        total: int = len(self.history)
        available: int = sum(1 for r in self.history if r.get("available"))
        times: List[float] = [
            r["response_time_ms"] for r in self.history
            if r.get("available") and r.get("response_time_ms") is not None
        ]
        by_category: Dict[str, int] = {}
        for record in self.history:
            key: str = record.get("category") or (
                CAT_OK if record.get("available") else CAT_UNREACHABLE)
            by_category[key] = by_category.get(key, 0) + 1
        return {
            "total": total,
            "available": available,
            "unavailable": total - available,
            "uptime_percent": (available / total * 100) if total else 0.0,
            "avg_time_ms": (sum(times) / len(times)) if times else None,
            "min_time_ms": min(times) if times else None,
            "max_time_ms": max(times) if times else None,
            "by_category": by_category,
        }

    def per_site_stats(self) -> List[Dict[str, Any]]:
        """Статистика по каждому сайту отдельно (самые проблемные - первыми)."""
        groups: Dict[str, List[CheckResult]] = {}
        for record in self.history:
            groups.setdefault(str(record["url"]), []).append(record)
        rows: List[Dict[str, Any]] = []
        for url, items in groups.items():
            ok_count: int = sum(1 for i in items if i.get("available"))
            times: List[float] = [i["response_time_ms"] for i in items
                                  if i.get("available") and i.get("response_time_ms") is not None]
            last: CheckResult = items[-1]  # записи хранятся в хронологическом порядке
            rows.append({
                "url": url,
                "checks": len(items),
                "uptime_percent": ok_count / len(items) * 100,
                "avg_time_ms": (sum(times) / len(times)) if times else None,
                "last_category": last.get("category") or (
                    CAT_OK if last.get("available") else CAT_UNREACHABLE),
            })
        rows.sort(key=lambda s: (s["uptime_percent"], s["url"]))
        return rows

    # ------------------------- Экспорт и импорт -------------------------
    def export_csv(self, path: Optional[Path] = None,
                   records: Optional[List[CheckResult]] = None) -> Path:
        """
        Сохраняет историю в CSV. Кодировка utf-8-sig и разделитель ';' -
        так файл без проблем открывается в Excel с русской локалью.
        """
        rows: List[CheckResult] = self.history if records is None else records
        if path is None:
            stamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = EXPORT_DIR / f"report_{stamp}.csv"
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=CSV_FIELDS, delimiter=";",
                                    extrasaction="ignore")
            writer.writeheader()
            for record in rows:
                line: Dict[str, Any] = {k: record.get(k, "") for k in CSV_FIELDS}
                line["available"] = "да" if record.get("available") else "нет"
                # None превращаем в пустую ячейку
                writer.writerow({k: ("" if v is None else v) for k, v in line.items()})
        return path

    @staticmethod
    def read_sites_file(path: Path) -> List[str]:
        """
        Читает список адресов из текстового файла.
        Пустые строки и комментарии (# ...) пропускаются.
        """
        text: str = Path(path).read_text(encoding="utf-8-sig")
        items: List[str] = []
        for line in text.splitlines():
            # Отрезаем комментарий в конце строки: «google.com  # поисковик»
            clean: str = re.split(r"\s#", line.strip(), maxsplit=1)[0].strip()
            if clean and not clean.startswith("#"):
                items.append(clean)
        return items

    # ---------------------------- Настройки ----------------------------
    @staticmethod
    def _sanitize_settings(raw: Any) -> Dict[str, int]:
        """Оставляет только корректные значения, остальное - по умолчанию."""
        result: Dict[str, int] = dict(DEFAULT_SETTINGS)
        if isinstance(raw, dict):
            for key, (low, high) in SETTINGS_LIMITS.items():
                value: Any = raw.get(key)
                # bool в Python - подкласс int, поэтому исключаем его явно
                if isinstance(value, int) and not isinstance(value, bool) and low <= value <= high:
                    result[key] = value
        return result

    def _load_settings(self) -> Dict[str, int]:
        """Загружает настройки; при любой проблеме берёт значения по умолчанию."""
        if not self.config_path.exists():
            return dict(DEFAULT_SETTINGS)
        try:
            with open(self.config_path, "r", encoding="utf-8") as file:
                return self._sanitize_settings(json.load(file))
        except (OSError, ValueError):
            return dict(DEFAULT_SETTINGS)

    def update_settings(self, **changes: int) -> bool:
        """Меняет одну или несколько настроек и сохраняет их."""
        merged: Dict[str, int] = dict(self.settings)
        merged.update(changes)
        self.settings = self._sanitize_settings(merged)
        return self._atomic_write(self.config_path, self.settings)

    def reset_settings(self) -> bool:
        """Возвращает настройки по умолчанию."""
        self.settings = dict(DEFAULT_SETTINGS)
        return self._atomic_write(self.config_path, self.settings)


# =====================================================================
#                          КЛАСС SiteChecker
# =====================================================================
class SiteChecker:
    """Асинхронная проверка доступности сайтов."""

    def __init__(self, timeout: int = DEFAULT_SETTINGS["timeout"],
                 max_concurrent: int = DEFAULT_SETTINGS["concurrency"],
                 retries: int = DEFAULT_SETTINGS["retries"]) -> None:
        """Сохраняет настройки, приводя их к допустимым значениям."""
        self.timeout_seconds: int = max(1, int(timeout))
        self.timeout: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        # Больше 10 одновременных запросов не допускаем ни при каких настройках
        self.max_concurrent: int = max(1, min(int(max_concurrent), MAX_CONCURRENT_LIMIT))
        self.retries: int = max(0, int(retries))

    # --------------------- Разбор и проверка адресов ---------------------
    @staticmethod
    def normalize_url(raw: str) -> Optional[str]:
        """
        Приводит введённую строку к корректному URL или возвращает None.
        Примеры: 'google.com' -> 'https://google.com',
                 'пример.рф'  -> 'https://xn--e1afmkfd.xn--p1ai'.
        """
        text: str = raw.strip()
        if not text or any(ch.isspace() for ch in text):
            return None
        # Если схема не указана - по умолчанию https
        if "://" not in text:
            text = "https://" + text
        try:
            parts = urlsplit(text)
            host: Optional[str] = parts.hostname
            port: Optional[int] = parts.port  # ValueError, если порт некорректный
        except ValueError:
            return None
        if parts.scheme.lower() not in ("http", "https") or not host:
            return None
        if parts.username or parts.password:  # логины в адресе не принимаем
            return None
        try:
            # Кириллические и другие национальные домены -> punycode
            ascii_host: str = host.encode("idna").decode("ascii")
        except UnicodeError:
            return None
        if not _HOST_RE.match(ascii_host):
            return None
        # Домен должен содержать точку (example.com); исключение - localhost
        if "." not in ascii_host and ascii_host != "localhost":
            return None
        netloc: str = ascii_host + (f":{port}" if port else "")
        return urlunsplit((parts.scheme.lower(), netloc, parts.path, parts.query, ""))

    @staticmethod
    def parse_urls(raw: str) -> Tuple[List[str], List[str]]:
        """
        Разбирает строку с несколькими адресами (через пробел, запятую, ';' или
        перенос строки). Возвращает (корректные без дублей, некорректные).
        """
        tokens: List[str] = [t for t in re.split(r"[\s,;]+", raw.strip()) if t]
        urls: List[str] = []
        invalid: List[str] = []
        seen: set = set()
        for token in tokens:
            url: Optional[str] = SiteChecker.normalize_url(token)
            if url is None:
                invalid.append(token)
            elif url not in seen:  # дубли убираем, порядок сохраняем
                seen.add(url)
                urls.append(url)
        return urls, invalid

    @staticmethod
    def classify(status_code: Optional[int]) -> str:
        """Определяет категорию результата по HTTP-коду."""
        if status_code is None:
            return CAT_UNREACHABLE
        if status_code in (401, 403, 429):
            return CAT_RESTRICTED  # сервер жив, просто не пускает бота
        if status_code >= 500:
            return CAT_SERVER_ERROR
        if status_code >= 400:
            return CAT_CLIENT_ERROR
        return CAT_OK  # 1xx, 2xx, 3xx

    @staticmethod
    def describe_error(exc: BaseException) -> Tuple[str, bool]:
        """
        Превращает исключение в понятный текст.
        Возвращает (сообщение, имеет ли смысл повторить запрос).
        Порядок проверок важен: сначала более частные классы ошибок.
        """
        if isinstance(exc, asyncio.TimeoutError):
            return "Таймаут: сервер не ответил вовремя", True
        if isinstance(exc, aiohttp.InvalidURL):
            return "Неверный URL", False
        if isinstance(exc, aiohttp.TooManyRedirects):
            return "Слишком много перенаправлений", False
        if isinstance(exc, aiohttp.ClientSSLError):
            return "Ошибка SSL-сертификата", False
        if isinstance(exc, aiohttp.ClientConnectorError):
            if isinstance(getattr(exc, "os_error", None), socket.gaierror):
                return "Домен не найден (ошибка DNS)", False
            return "Не удалось подключиться к серверу", True
        if isinstance(exc, aiohttp.ClientError):
            return f"Ошибка соединения ({type(exc).__name__})", True
        if isinstance(exc, UnicodeError):
            return "Неверный адрес", False
        LOG.warning("Непредвиденная ошибка при проверке: %r", exc)
        return f"Непредвиденная ошибка: {type(exc).__name__}", False

    # ----------------------------- Запросы -----------------------------
    async def _request(self, session: aiohttp.ClientSession, method: str,
                       url: str) -> Tuple[int, str, int]:
        """Один запрос. Возвращает (HTTP-код, итоговый URL, число редиректов)."""
        async with session.request(method, url, allow_redirects=True,
                                   max_redirects=10) as response:
            return response.status, str(response.url), len(response.history)

    async def check_site(self, session: aiohttp.ClientSession, url: str,
                         semaphore: asyncio.Semaphore) -> CheckResult:
        """
        Проверяет один сайт.
        1) Сначала метод HEAD - он быстрее, тело страницы не скачивается.
        2) Если сервер ответил 405 (HEAD запрещён) - повторяем методом GET.
        3) При сетевой ошибке делаем ещё до `retries` попыток.
        Исключения не выходят наружу: любая ошибка превращается в результат.
        """
        # Семафор не даёт запустить больше max_concurrent проверок одновременно
        async with semaphore:
            status: Optional[int] = None
            method: str = "HEAD"
            error: Optional[str] = None
            final_url: str = url
            redirects: int = 0
            started: float = time.perf_counter()

            for attempt in range(self.retries + 1):
                started = time.perf_counter()  # время меряем только по последней попытке
                method = "HEAD"
                try:
                    status, final_url, redirects = await self._request(session, "HEAD", url)
                    if status in FALLBACK_STATUSES:
                        method = "GET"
                        status, final_url, redirects = await self._request(session, "GET", url)
                    error = None
                    break  # получили ответ сервера - повторять не нужно
                except Exception as exc:  # noqa: BLE001 - защита: сеть не должна ронять программу
                    status = None
                    error, retryable = self.describe_error(exc)
                    if not retryable or attempt == self.retries:
                        break
                    await asyncio.sleep(RETRY_DELAY)

            elapsed_ms: float = round((time.perf_counter() - started) * 1000, 1)
            category: str = self.classify(status)
            return {
                "url": url,
                "final_url": final_url,
                "redirects": redirects,
                # «Доступен» = сервер ответил нормально или просто ограничил доступ
                "available": category in (CAT_OK, CAT_RESTRICTED),
                "category": category,
                "status_code": status,
                "method": method,
                "response_time_ms": elapsed_ms if status is not None else None,
                "error": error,
                "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

    async def check_many(
            self, urls: List[str],
            on_result: Optional[Callable[[CheckResult], None]] = None,
    ) -> List[CheckResult]:
        """
        Проверяет список сайтов параллельно (не более max_concurrent сразу).
        on_result вызывается после каждой завершённой проверки (для прогресс-бара).
        Порядок результатов совпадает с порядком адресов.
        """
        if not urls:
            return []
        semaphore = asyncio.Semaphore(self.max_concurrent)
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        headers: Dict[str, str] = {"User-Agent": USER_AGENT}

        # Одна общая сессия на все запросы - так быстрее (переиспользуются соединения)
        async with aiohttp.ClientSession(connector=connector, timeout=self.timeout,
                                         headers=headers) as session:
            async def worker(url: str) -> CheckResult:
                result: CheckResult = await self.check_site(session, url, semaphore)
                if on_result is not None:
                    on_result(result)
                return result

            # gather запускает все проверки сразу, а семафор ограничивает нагрузку
            return list(await asyncio.gather(*(worker(u) for u in urls)))


# =====================================================================
#                              КЛАСС App
# =====================================================================
class App:
    """Консольный интерфейс: меню, ввод с проверкой и красивый вывод."""

    def __init__(self, data: Optional[DataManager] = None,
                 console: Optional[Console] = None,
                 timeout_override: Optional[int] = None) -> None:
        """Создаёт менеджер данных, консоль и проверщик сайтов."""
        self.console: Console = console or Console()
        self.data: DataManager = data or DataManager()
        self.timeout_override: Optional[int] = timeout_override
        self.checker: SiteChecker = self._make_checker()

    def _make_checker(self) -> SiteChecker:
        """Создаёт SiteChecker по текущим настройкам."""
        s: Dict[str, int] = self.data.settings
        return SiteChecker(timeout=self.timeout_override or s["timeout"],
                           max_concurrent=s["concurrency"], retries=s["retries"])

    # ------------------------ Ввод с проверкой ------------------------
    def _ask(self, prompt: str) -> str:
        """Запрашивает строку у пользователя (prompt - наш фиксированный текст)."""
        return self.console.input(f"[bold cyan]{prompt}[/] ").strip()

    def _ask_choice(self, prompt: str, choices: Sequence[str]) -> str:
        """Спрашивает, пока пользователь не выберет один из допустимых вариантов."""
        hint: str = "/".join(choices)
        while True:
            answer: str = self._ask(f"{prompt} ({hint}):")
            if answer in choices:
                return answer
            self.console.print(f"[red]✖ Неверный ввод. Допустимо: {hint}[/]")

    def _ask_int(self, prompt: str, low: int, high: int) -> int:
        """Спрашивает целое число в диапазоне [low, high]."""
        while True:
            answer: str = self._ask(f"{prompt} ({low}-{high}):")
            try:
                value: int = int(answer)
            except ValueError:  # ввели буквы или пустую строку
                value = low - 1
            if low <= value <= high:
                return value
            self.console.print(f"[red]✖ Введите целое число от {low} до {high}.[/]")

    def _ask_yes_no(self, prompt: str) -> bool:
        """Вопрос «да/нет». Понимает y/n и д/н."""
        while True:
            answer: str = self._ask(f"{prompt} (да/нет):").lower()
            if answer in ("y", "yes", "д", "да"):
                return True
            if answer in ("n", "no", "н", "нет"):
                return False
            self.console.print("[red]✖ Ответьте «да» или «нет».[/]")

    def _pause(self) -> None:
        """Ждёт Enter, чтобы пользователь успел прочитать результат."""
        self.console.input("\n[dim]Нажмите Enter, чтобы вернуться в меню...[/]")

    # --------------------------- Оформление ---------------------------
    def _banner(self) -> Panel:
        """Верхняя «шапка» программы."""
        title = Text("SITE AVAILABILITY CHECKER", style="bold white", justify="center")
        subtitle = Text("Асинхронная проверка доступности сайтов  ·  asyncio + aiohttp",
                        style="cyan", justify="center")
        return Panel(Group(title, subtitle), box=box.DOUBLE,
                     border_style="bright_blue", padding=(1, 4))

    def _status_line(self) -> Text:
        """Строка состояния под шапкой."""
        s: Dict[str, int] = self.data.settings
        return Text(
            f"История: {len(self.data.history)} зап.  ·  Таймаут: "
            f"{self.timeout_override or s['timeout']} с  ·  Параллельно: "
            f"{s['concurrency']}  ·  Повторов: {s['retries']}",
            style="dim", justify="center")

    def _render_menu(self) -> None:
        """Выводит главное меню."""
        self.console.clear()
        self.console.print(self._banner())
        self.console.print(self._status_line())
        if self.data.load_warning:  # показываем предупреждение один раз
            self.console.print(Panel(Text(self.data.load_warning), border_style="yellow",
                                     title="Внимание"))
            self.data.load_warning = None
        menu = Table(show_header=False, box=box.ROUNDED, border_style="blue",
                     padding=(0, 2))
        menu.add_column(justify="right", style="bold cyan")
        menu.add_column()
        items: List[Tuple[str, str]] = [
            ("1", "Проверить один сайт"),
            ("2", "Проверить несколько сайтов"),
            ("3", "Проверить сайты из файла (sites.txt)"),
            ("4", "История: поиск, фильтры, сортировка"),
            ("5", "Статистика"),
            ("6", "Экспорт истории в CSV"),
            ("7", "Настройки"),
            ("8", "Очистить историю"),
            ("0", "Выход"),
        ]
        for number, label in items:
            menu.add_row(number, label)
        self.console.print(Align.center(menu))

    @staticmethod
    def _format_time(ms: Optional[float]) -> Text:
        """Время ответа с цветом: зелёный - быстро, жёлтый - средне, красный - медленно."""
        if ms is None:
            return Text("—", style="dim")
        style: str = "green" if ms < 300 else ("yellow" if ms < 1000 else "red")
        return Text(f"{ms:.0f} мс", style=style)

    @staticmethod
    def _category_of(record: CheckResult) -> str:
        """Категория записи (для старых записей без категории - вычисляем)."""
        return record.get("category") or (
            CAT_OK if record.get("available") else CAT_UNREACHABLE)

    def _details(self, record: CheckResult) -> Text:
        """Короткое пояснение к результату."""
        if record.get("error"):
            return Text(str(record["error"]), style="red")
        notes: List[str] = [str(record.get("method", "HEAD"))]
        if record.get("method") == "GET":
            notes.append("HEAD не поддерживается")
        if record.get("redirects"):
            notes.append(f"редиректов: {record['redirects']}")
        if self._category_of(record) == CAT_RESTRICTED:
            notes.append("сайт жив, но ограничивает доступ ботам")
        return Text(" · ".join(notes), style="dim")

    def _results_table(self, results: List[CheckResult], title: str) -> Table:
        """Таблица результатов проверки."""
        table = Table(title=title, box=box.ROUNDED, header_style="bold cyan",
                      border_style="blue", expand=True, title_style="bold")
        table.add_column("#", justify="right", style="dim", no_wrap=True)
        table.add_column("Статус", no_wrap=True)
        table.add_column("Сайт", overflow="fold", ratio=3)
        table.add_column("Код", justify="right", no_wrap=True)
        table.add_column("Время", justify="right", no_wrap=True)
        table.add_column("Детали", overflow="fold", ratio=2)
        for index, record in enumerate(results, start=1):
            category: str = self._category_of(record)
            code: str = str(record["status_code"]) if record.get("status_code") is not None else "—"
            table.add_row(
                str(index),
                Text(f"● {CATEGORY_TITLES[category]}", style=CATEGORY_STYLES[category]),
                Text(str(record["url"])),  # Text - чтобы символы [] в адресе не считались разметкой
                Text(code),
                self._format_time(record.get("response_time_ms")),
                self._details(record),
            )
        return table

    def _summary_panel(self, results: List[CheckResult], elapsed: float) -> Panel:
        """Панель «Итоги» после проверки."""
        total: int = len(results)
        ok_count: int = sum(1 for r in results if r["available"])
        timed: List[CheckResult] = [r for r in results if r.get("response_time_ms") is not None]
        grid = Table.grid(padding=(0, 2))
        grid.add_column(style="dim")
        grid.add_column()
        grid.add_row("Проверено", Text(str(total)))
        grid.add_row("Доступно", Text(f"{ok_count} из {total} ({ok_count / total * 100:.0f}%)",
                                      style="green" if ok_count == total else "yellow"))
        if total - ok_count:
            grid.add_row("Недоступно", Text(str(total - ok_count), style="red"))
        if timed:
            avg: float = sum(r["response_time_ms"] for r in timed) / len(timed)
            fastest: CheckResult = min(timed, key=lambda r: r["response_time_ms"])
            slowest: CheckResult = max(timed, key=lambda r: r["response_time_ms"])
            grid.add_row("Среднее время", Text(f"{avg:.0f} мс"))
            grid.add_row("Быстрее всех", Text(f"{fastest['url']}  ({fastest['response_time_ms']:.0f} мс)"))
            grid.add_row("Медленнее всех", Text(f"{slowest['url']}  ({slowest['response_time_ms']:.0f} мс)"))
        grid.add_row("Общее время", Text(f"{elapsed:.2f} с"))
        if total > 1 and timed and elapsed > 0:
            # Сколько заняла бы проверка по очереди - наглядно показывает выгоду asyncio
            sequential: float = sum(r["response_time_ms"] for r in timed) / 1000
            grid.add_row("По очереди было бы",
                         Text(f"≈ {sequential:.1f} с  (асинхронность быстрее в ×{sequential / elapsed:.1f})",
                              style="cyan"))
        return Panel(grid, title="Итоги", box=box.ROUNDED,
                     border_style="green" if ok_count == total else "yellow")

    # ----------------------- Выполнение проверок -----------------------
    def _execute(self, urls: List[str], save: bool = True) -> List[CheckResult]:
        """Запускает проверку с прогресс-баром, выводит таблицу и итоги."""
        self.console.print(f"\n[dim]Проверяем адресов: {len(urls)}. "
                           f"Одновременно не более {self.checker.max_concurrent}.[/]")
        started: float = time.perf_counter()
        with Progress(SpinnerColumn(), TextColumn("[bold blue]Проверка"), BarColumn(),
                      MofNCompleteColumn(), TimeElapsedColumn(),
                      console=self.console, transient=True) as progress:
            task_id = progress.add_task("check", total=len(urls))
            results: List[CheckResult] = asyncio.run(
                self.checker.check_many(urls, lambda _r: progress.advance(task_id)))
        elapsed: float = time.perf_counter() - started

        self.console.print(self._results_table(results, "Результаты проверки"))
        self.console.print(self._summary_panel(results, elapsed))
        if save:
            if self.data.add_records(results):
                self.console.print("[dim]Результаты сохранены в историю.[/]")
            else:
                self.console.print("[yellow]⚠ Не удалось сохранить историю (см. site_checker.log).[/]")
        return results

    def run_batch(self, targets: Sequence[str], file: Optional[str] = None,
                  save: bool = True) -> int:
        """
        Режим командной строки (без меню).
        Код возврата: 0 - все сайты доступны, 1 - есть недоступные, 2 - ошибка ввода.
        """
        raw_items: List[str] = list(targets)
        if file:
            try:
                raw_items += self.data.read_sites_file(Path(file))
            except (OSError, ValueError) as error:
                self.console.print(f"[red]✖ Не удалось прочитать файл {escape(str(file))}: "
                                   f"{escape(str(error))}[/]")
                return 2
        urls, invalid = self.checker.parse_urls(" ".join(raw_items))
        if invalid:
            self.console.print(f"[yellow]⚠ Пропущены некорректные адреса: "
                               f"{escape(', '.join(invalid))}[/]")
        if not urls:
            self.console.print("[red]✖ Нет ни одного корректного адреса для проверки.[/]")
            return 2
        results: List[CheckResult] = self._execute(urls, save=save)
        return 0 if all(r["available"] for r in results) else 1

    # --------------------------- Пункты меню ---------------------------
    def _section(self, title: str) -> None:
        """Заголовок раздела."""
        self.console.print()
        self.console.rule(f"[bold cyan]{title}[/]")

    def _check_one(self) -> None:
        """Пункт 1: проверка одного сайта."""
        self._section("Проверка одного сайта")
        while True:
            raw: str = self._ask("Адрес сайта (например, google.com), 0 - отмена:")
            if raw == "0":
                return
            url: Optional[str] = self.checker.normalize_url(raw)
            if url is not None:
                break
            self.console.print("[red]✖ Некорректный адрес. Пример: google.com или https://github.com[/]")
        self._execute([url])
        self._pause()

    def _check_many(self) -> None:
        """Пункт 2: проверка нескольких сайтов, введённых через пробел или запятую."""
        self._section("Проверка нескольких сайтов")
        self.console.print("[dim]Введите адреса через пробел или запятую. 0 - отмена.[/]")
        while True:
            raw: str = self._ask("Адреса:")
            if raw == "0":
                return
            urls, invalid = self.checker.parse_urls(raw)
            if invalid:
                self.console.print(f"[yellow]⚠ Пропущены некорректные адреса: "
                                   f"{escape(', '.join(invalid))}[/]")
            if urls:
                break
            self.console.print("[red]✖ Нет ни одного корректного адреса. Попробуйте ещё раз.[/]")
        self._execute(urls)
        self._pause()

    def _check_file(self) -> None:
        """Пункт 3: проверка сайтов из текстового файла."""
        self._section("Проверка сайтов из файла")
        while True:
            raw: str = self._ask(f"Путь к файлу (Enter - {DEFAULT_SITES_FILE.name}), 0 - отмена:")
            if raw == "0":
                return
            path: Path = Path(raw.strip('"')) if raw else DEFAULT_SITES_FILE
            try:
                lines: List[str] = self.data.read_sites_file(path)
            except FileNotFoundError:
                self.console.print(f"[red]✖ Файл не найден: {escape(str(path))}[/]")
                continue
            except (OSError, ValueError) as error:
                self.console.print(f"[red]✖ Не удалось прочитать файл: {escape(str(error))}[/]")
                continue
            urls, invalid = self.checker.parse_urls(" ".join(lines))
            if invalid:
                self.console.print(f"[yellow]⚠ Пропущены некорректные строки: "
                                   f"{escape(', '.join(invalid))}[/]")
            if urls:
                break
            self.console.print("[red]✖ В файле нет корректных адресов.[/]")
        self._execute(urls)
        self._pause()

    def _history_table(self, rows: List[CheckResult], title: str) -> Table:
        """Таблица истории."""
        table = Table(title=title, box=box.ROUNDED, header_style="bold cyan",
                      border_style="blue", expand=True, title_style="bold")
        table.add_column("Дата и время", no_wrap=True)
        table.add_column("Статус", no_wrap=True)
        table.add_column("Сайт", overflow="fold", ratio=3)
        table.add_column("Код", justify="right", no_wrap=True)
        table.add_column("Время", justify="right", no_wrap=True)
        for record in rows:
            category: str = self._category_of(record)
            code: str = str(record["status_code"]) if record.get("status_code") is not None else "—"
            table.add_row(Text(str(record.get("checked_at", "?")), style="dim"),
                          Text(f"● {CATEGORY_TITLES[category]}", style=CATEGORY_STYLES[category]),
                          Text(str(record["url"])), Text(code),
                          self._format_time(record.get("response_time_ms")))
        return table

    def _show_history(self) -> None:
        """Пункт 4: история с поиском, фильтром и сортировкой."""
        self._section("История проверок")
        if not self.data.history:
            self.console.print("История пуста. Сначала проверьте какие-нибудь сайты.")
            self._pause()
            return
        self.console.print("[bold]Фильтр:[/]  1 - все   2 - только доступные   "
                           "3 - только недоступные   4 - поиск по адресу")
        flt: str = self._ask_choice("Фильтр", ["1", "2", "3", "4"])
        only: str = {"1": "all", "2": "available", "3": "unavailable", "4": "all"}[flt]
        text: str = ""
        if flt == "4":
            while not text:
                text = self._ask("Часть адреса для поиска:")
        self.console.print("[bold]Сортировка:[/]  1 - новые сверху   2 - старые сверху   "
                           "3 - самые медленные   4 - самые быстрые   5 - по адресу А-Я")
        srt: str = self._ask_choice("Сортировка", ["1", "2", "3", "4", "5"])
        sort: str = {"1": "newest", "2": "oldest", "3": "slowest",
                     "4": "fastest", "5": "url"}[srt]
        rows: List[CheckResult] = self.data.query(text=text, only=only, sort=sort)
        if not rows:
            self.console.print("[yellow]По такому запросу записей нет.[/]")
        else:
            shown: List[CheckResult] = rows[:HISTORY_PAGE_SIZE]
            title: str = f"Показано {len(shown)} из {len(rows)} (всего в истории: {len(self.data.history)})"
            self.console.print(self._history_table(shown, title))
        self._pause()

    @staticmethod
    def _uptime_bar(percent: float) -> Text:
        """Мини-диаграмма доступности из блоков."""
        filled: int = round(percent / 10)
        style: str = "green" if percent >= 90 else ("yellow" if percent >= 50 else "red")
        return Text("█" * filled + "░" * (10 - filled) + f" {percent:5.1f}%", style=style)

    def _show_statistics(self) -> None:
        """Пункт 5: статистика по всей истории."""
        self._section("Статистика")
        stats: Dict[str, Any] = self.data.get_statistics()
        if stats["total"] == 0:
            self.console.print("Данных пока нет.")
            self._pause()
            return
        grid = Table.grid(padding=(0, 2))
        grid.add_column(style="dim")
        grid.add_column()
        grid.add_row("Всего проверок", Text(str(stats["total"])))
        grid.add_row("Доступность", self._uptime_bar(stats["uptime_percent"]))
        grid.add_row("Доступно / нет", Text(f"{stats['available']} / {stats['unavailable']}"))
        if stats["avg_time_ms"] is not None:
            grid.add_row("Время ответа", Text(
                f"среднее {stats['avg_time_ms']:.0f} мс, лучшее {stats['min_time_ms']:.0f} мс, "
                f"худшее {stats['max_time_ms']:.0f} мс"))
        for key, count in sorted(stats["by_category"].items()):
            grid.add_row(CATEGORY_TITLES.get(key, key).capitalize(), Text(str(count)))
        self.console.print(Panel(grid, title="Общая статистика", box=box.ROUNDED,
                                 border_style="blue"))

        table = Table(title="По каждому сайту (проблемные сверху)", box=box.ROUNDED,
                      header_style="bold cyan", border_style="blue", expand=True)
        table.add_column("Сайт", overflow="fold", ratio=3)
        table.add_column("Проверок", justify="right", no_wrap=True)
        table.add_column("Доступность", no_wrap=True)
        table.add_column("Среднее", justify="right", no_wrap=True)
        table.add_column("Последний статус", no_wrap=True)
        for site in self.data.per_site_stats()[:15]:
            cat: str = site["last_category"]
            table.add_row(Text(site["url"]), str(site["checks"]),
                          self._uptime_bar(site["uptime_percent"]),
                          self._format_time(site["avg_time_ms"]),
                          Text(CATEGORY_TITLES.get(cat, cat), style=CATEGORY_STYLES.get(cat, "")))
        self.console.print(table)
        self._pause()

    def _export_csv(self) -> None:
        """Пункт 6: экспорт истории в CSV."""
        self._section("Экспорт в CSV")
        if not self.data.history:
            self.console.print("История пуста - экспортировать нечего.")
        else:
            try:
                path: Path = self.data.export_csv()
                self.console.print(f"[green]✔ Готово![/] Записей: {len(self.data.history)}\n"
                                   f"Файл: {escape(str(path))}")
            except OSError as error:
                self.console.print(f"[red]✖ Не удалось сохранить файл: {escape(str(error))}[/]")
        self._pause()

    def _settings(self) -> None:
        """Пункт 7: настройки (таймаут, параллельность, повторы)."""
        while True:
            self._section("Настройки")
            s: Dict[str, int] = self.data.settings
            table = Table(show_header=False, box=box.ROUNDED, border_style="blue")
            table.add_column(style="bold cyan", justify="right")
            table.add_column()
            table.add_column(style="bold")
            table.add_row("1", "Таймаут запроса, секунд (1-60)", str(s["timeout"]))
            table.add_row("2", "Одновременных запросов (1-10)", str(s["concurrency"]))
            table.add_row("3", "Повторных попыток при сбое (0-3)", str(s["retries"]))
            table.add_row("4", "Сбросить настройки по умолчанию", "")
            table.add_row("0", "Назад", "")
            self.console.print(table)
            choice: str = self._ask_choice("Что изменить", ["0", "1", "2", "3", "4"])
            if choice == "0":
                return
            if choice == "1":
                self.data.update_settings(timeout=self._ask_int("Таймаут", *SETTINGS_LIMITS["timeout"]))
            elif choice == "2":
                self.data.update_settings(concurrency=self._ask_int("Параллельных запросов", *SETTINGS_LIMITS["concurrency"]))
            elif choice == "3":
                self.data.update_settings(retries=self._ask_int("Повторных попыток", *SETTINGS_LIMITS["retries"]))
            else:
                self.data.reset_settings()
            self.checker = self._make_checker()  # применяем новые настройки сразу
            self.console.print("[green]✔ Настройки сохранены.[/]")

    def _clear_history(self) -> None:
        """Пункт 8: очистка истории с подтверждением."""
        self._section("Очистка истории")
        if self._ask_yes_no(f"Удалить все записи ({len(self.data.history)})?"):
            self.data.clear()
            self.console.print("[green]✔ История очищена.[/]")
        else:
            self.console.print("Отменено.")
        self._pause()

    # --------------------------- Главный цикл ---------------------------
    def run(self) -> None:
        """Главный цикл: показать меню -> получить выбор -> выполнить действие."""
        # Словарь «номер пункта -> метод» заменяет длинную цепочку if/elif
        actions: Dict[str, Callable[[], None]] = {
            "1": self._check_one,
            "2": self._check_many,
            "3": self._check_file,
            "4": self._show_history,
            "5": self._show_statistics,
            "6": self._export_csv,
            "7": self._settings,
            "8": self._clear_history,
        }
        while True:
            self._render_menu()
            choice: str = self._ask_choice("Ваш выбор", ["0", "1", "2", "3", "4", "5", "6", "7", "8"])
            if choice == "0":
                self.console.print("[bold cyan]До свидания![/]")
                return
            try:
                actions[choice]()
            except KeyboardInterrupt:
                # Ctrl+C внутри пункта меню - отмена действия, а не выход из программы
                self.console.print("\n[yellow]Действие отменено.[/]")
                self._pause()
            except Exception as error:  # noqa: BLE001 - последняя линия обороны
                LOG.exception("Ошибка в пункте меню %s", choice)
                self.console.print(f"[red]✖ Произошла ошибка: {escape(str(error))}[/]")
                self._pause()


# =====================================================================
#                         ТОЧКА ВХОДА И CLI
# =====================================================================
def _timeout_arg(value: str) -> int:
    """Проверка аргумента --timeout для argparse."""
    low, high = SETTINGS_LIMITS["timeout"]
    try:
        number: int = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"нужно целое число от {low} до {high}")
    if not low <= number <= high:
        raise argparse.ArgumentTypeError(f"нужно целое число от {low} до {high}")
    return number


def build_parser() -> argparse.ArgumentParser:
    """Описание аргументов командной строки."""
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Асинхронная проверка доступности сайтов. "
                    "Без аргументов запускается интерактивное меню.")
    parser.add_argument("targets", nargs="*", metavar="сайт",
                        help="адреса для проверки без меню (например: google.com github.com)")
    parser.add_argument("-f", "--file", metavar="ФАЙЛ",
                        help="файл со списком адресов (по одному в строке)")
    parser.add_argument("-t", "--timeout", type=_timeout_arg, metavar="СЕК",
                        help="таймаут одного запроса, 1-60 секунд")
    parser.add_argument("--no-save", action="store_true",
                        help="не записывать результаты в историю")
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s {VERSION}")
    return parser


def _configure_stdio() -> None:
    """Включает UTF-8 в консоли (в старых Windows иначе возможны «кракозябры»)."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
        except (AttributeError, ValueError, OSError):
            pass


def _setup_logging() -> None:
    """Настраивает запись технических ошибок в файл."""
    try:
        logging.basicConfig(filename=LOG_FILE, level=logging.INFO, encoding="utf-8",
                            format="%(asctime)s %(levelname)s %(message)s")
    except OSError:
        pass  # нет прав на запись - работаем без лог-файла


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Точка входа в программу. Возвращает код завершения."""
    _configure_stdio()
    _setup_logging()
    args = build_parser().parse_args(argv)
    try:
        app = App(timeout_override=args.timeout)
        if args.targets or args.file:
            return app.run_batch(args.targets, args.file, save=not args.no_save)
        app.run()
        return 0
    except (KeyboardInterrupt, EOFError):
        # Ctrl+C или Ctrl+D - выходим аккуратно, без трейсбека
        print("\nПрограмма завершена пользователем.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
