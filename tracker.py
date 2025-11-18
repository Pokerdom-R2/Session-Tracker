#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Оффлайн-трекер покерных сессий Покердом.
Скрипт:
- читает локальные файлы раздач,
- парсит каждую раздачу,
- определяет время/лимит/результат,
- группирует раздачи по сессиям,
- строит отчёты и экспортирует данные.
"""

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from datetime import datetime, timedelta

# =========================
#       CLI аргументы
# =========================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Оффлайн-трекер покерных сессий Покердом."
    )

    parser.add_argument(
        "--hands-path", required=True,
        help="Путь к папке с файлами раздач"
    )
    parser.add_argument(
        "--hero-name", required=True,
        help="Ник героя"
    )
    parser.add_argument(
        "--session-gap-minutes", type=int, default=30,
        help="Разрыв между раздачами, после которого начинается новая сессия (по умолчанию 30 минут)"
    )
    parser.add_argument(
        "--recursive", action="store_true",
        help="Рекурсивно обходить папки"
    )
    parser.add_argument(
        "--all-files", action="store_true",
        help="Обрабатывать все файлы (а не только *.txt)"
    )
    parser.add_argument(
        "--encoding", default="utf-8",
        help="Кодировка входных файлов (по умолчанию utf-8, можно cp1251)"
    )
    parser.add_argument(
        "--export-csv",
        help="Экспорт сессий в CSV файл"
    )
    parser.add_argument(
        "--export-json",
        help="Экспорт всех данных в JSON"
    )
    parser.add_argument(
        "--report", nargs="*", default=["summary", "sessions"],
        help="Какие отчёты выводить: summary sessions limits"
    )

    return parser.parse_args()


# =========================
#   Чтение файлов раздач
# =========================

def iter_hand_files(base_path: Path, recursive: bool, all_files: bool):
    """Итерирует файлы раздач в указанной папке"""
    if recursive:
        files = base_path.rglob("*")
    else:
        files = base_path.glob("*")

    for f in files:
        if not f.is_file():
            continue
        if not all_files and f.suffix.lower() != ".txt":
            continue
        yield f


# =========================
#      Парсер раздач
# =========================

def split_raw_hands(text: str):
    """
    Делит содержимое файла на отдельные раздачи.
    Предполагаем, что между раздачами одна или более пустых строк.
    """
    blocks = re.split(r"\n\s*\n", text.strip())
    return [b.strip() for b in blocks if b.strip()]


def parse_hand(raw_hand: str, hero_name: str):
    """
    Возвращает словарь со структурой раздачи.
    """

    # --------------------------
    # 1. Дата/время раздачи
    # --------------------------
    # Попытка найти дату формата: YYYY-MM-DD HH:MM:SS
    date_match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", raw_hand)
    if date_match:
        hand_time = datetime.strptime(date_match.group(1), "%Y-%m-%d %H:%M:%S")
    else:
        # fallback — если не нашли дату
        hand_time = None

    # --------------------------
    # 2. Лимит (пример: NL25, NL50)
    # --------------------------
    limit_match = re.search(r"(NL|PL)\s?(\d+)", raw_hand, re.IGNORECASE)
    limit = limit_match.group(0).upper() if limit_match else "unknown"

    # --------------------------
    # 3. Бай-ин BB (например, в NL25 big blind = 0.25)
    # --------------------------
    bb_size = None
    if limit_match:
        try:
            bb_size = float(limit_match.group(2)) / 100
        except Exception:
            bb_size = None

    # --------------------------
    # 4. Результат героя в деньгах
    # --------------------------
    # Пример упрощённого поиска:
    # MyHero won 1.25
    result_money = 0.0
    hero_pattern = re.compile(rf"{re.escape(hero_name)}.*?(won|lost)\s([+-]?\d+(\.\d+)?)", re.IGNORECASE)
    hero_match = hero_pattern.search(raw_hand)

    if hero_match:
        sign = 1 if hero_match.group(1).lower() == "won" else -1
        result_money = sign * float(hero_match.group(2))

    # --------------------------
    # 5. Результат в BB
    # --------------------------
    result_bb = None
    if bb_size and bb_size > 0:
        result_bb = result_money / bb_size

    # --------------------------
    # 6. ID раздачи (если есть)
    # --------------------------
    hand_id_match = re.search(r"Hand\s*#(\d+)", raw_hand, re.IGNORECASE)
    hand_id = hand_id_match.group(1) if hand_id_match else id(raw_hand)

    return {
        "hand_id": hand_id,
        "datetime": hand_time,
        "limit": limit,
        "bb_size": bb_size,
        "hero_result_money": result_money,
        "hero_result_bb": result_bb,
        "raw": raw_hand
    }


# =========================
#   Формирование сессий
# =========================

def build_sessions(hands, session_gap_minutes: int):
    """
    Формирует сессии на основе времени раздач.
    """
    # Убираем раздачи без времени
    hands = [h for h in hands if h["datetime"] is not None]

    # Сортируем по времени
    hands.sort(key=lambda x: x["datetime"])

    sessions = []
    current = []

    gap = timedelta(minutes=session_gap_minutes)

    prev_time = None

    for hand in hands:
        if prev_time is None:
            # начинаем первую сессию
            current = [hand]
        else:
            if hand["datetime"] - prev_time > gap:
                # разрыв — завершаем текущую сессию
                sessions.append(current)
                current = [hand]
            else:
                current.append(hand)

        prev_time = hand["datetime"]

    if current:
        sessions.append(current)

    # Конвертируем сессии в структурированный формат
    result = []
    for idx, sess in enumerate(sessions, start=1):
        start_time = sess[0]["datetime"]
        end_time = sess[-1]["datetime"]
        duration = (end_time - start_time).total_seconds() / 60
        hands_count = len(sess)

        # лимиты
        limits = list({h["limit"] for h in sess})
        limit = limits[0] if len(limits) == 1 else "mixed"

        # totals
        money_total = sum(h["hero_result_money"] for h in sess)

        bb_results = [h["hero_result_bb"] for h in sess if h["hero_result_bb"] is not None]
        bb_total = sum(bb_results) if bb_results else None

        if bb_total is not None:
            bb_per_100 = (bb_total / hands_count) * 100
        else:
            bb_per_100 = None

        result.append({
            "session_id": idx,
            "start_time": start_time,
            "end_time": end_time,
            "duration_minutes": round(duration),
            "hands_count": hands_count,
            "limit": limit,
            "total_result_money": round(money_total, 2),
            "total_result_bb": round(bb_total, 2) if bb_total is not None else None,
            "bb_per_100": round(bb_per_100, 2) if bb_per_100 is not None else None,
            "hands": sess
        })

    return result


# =========================
#      Статистика лимитов
# =========================

def build_limits_stats(sessions):
    """Группирует статистику по лимитам"""
    stats = {}

    for sess in sessions:
        limit = sess["limit"]
        if limit == "mixed":
            continue  # не учитываем смешанные

        if limit not in stats:
            stats[limit] = {
                "limit": limit,
                "sessions_count": 0,
                "hands_count": 0,
                "total_result_money": 0.0,
                "total_result_bb": 0.0,
            }

        stats[limit]["sessions_count"] += 1
        stats[limit]["hands_count"] += sess["hands_count"]
        stats[limit]["total_result_money"] += sess["total_result_money"]
        if sess["total_result_bb"] is not None:
            stats[limit]["total_result_bb"] += sess["total_result_bb"]

    # bb/100
    results = []
    for limit, d in stats.items():
        if d["hands_count"] > 0:
            bb100 = (d["total_result_bb"] / d["hands_count"]) * 100 if d["total_result_bb"] else None
        else:
            bb100 = None

        d["bb_per_100"] = round(bb100, 2) if bb100 is not None else None
        results.append(d)

    return results


# =========================
#     SUMMARY отчёт
# =========================

def build_summary(hands, sessions):
    if not hands:
        return {
            "total_hands": 0,
            "total_sessions": 0,
            "total_result_money": 0,
            "total_result_bb": None,
            "overall_bb_per_100": None,
            "first_hand_time": None,
            "last_hand_time": None
        }

    total_hands = len(hands)
    total_sessions = len(sessions)

    money_total = sum(h["hero_result_money"] for h in hands)

    bb_list = [h["hero_result_bb"] for h in hands if h["hero_result_bb"] is not None]
    bb_total = sum(bb_list) if bb_list else None

    if bb_total is not None:
        bb_per_100 = (bb_total / total_hands) * 100
    else:
        bb_per_100 = None

    times = [h["datetime"] for h in hands if h["datetime"] is not None]

    return {
        "total_hands": total_hands,
        "total_sessions": total_sessions,
        "total_result_money": round(money_total, 2),
        "total_result_bb": round(bb_total, 2) if bb_total is not None else None,
        "overall_bb_per_100": round(bb_per_100, 2) if bb_per_100 is not None else None,
        "first_hand_time": min(times) if times else None,
        "last_hand_time": max(times) if times else None
    }


# =========================
#      PRINT отчёты
# =========================

def print_summary(summary):
    print("\n=== SUMMARY ===")
    print(f"Раздач: {summary['total_hands']}")
    print(f"Сессий: {summary['total_sessions']}")
    print(f"Профит (денег): {summary['total_result_money']}")
    print(f"Профит (bb): {summary['total_result_bb']}")
    print(f"bb/100: {summary['overall_bb_per_100']}")
    print(f"Дата первой раздачи: {summary['first_hand_time']}")
    print(f"Дата последней раздачи: {summary['last_hand_time']}")


def print_sessions_report(sessions):
    print("\n=== SESSIONS ===")
    print("ID | Start | Hands | Limit | Result | Result(bb) | bb/100 | Duration(min)")
    for s in sessions:
        print(
            f"{s['session_id']} | "
            f"{s['start_time']} | "
            f"{s['hands_count']} | "
            f"{s['limit']} | "
            f"{s['total_result_money']} | "
            f"{s['total_result_bb']} | "
            f"{s['bb_per_100']} | "
            f"{s['duration_minutes']}"
        )


def print_limits_report(limits):
    print("\n=== LIMITS ===")
    print("Limit | Sessions | Hands | Result | Result(bb) | bb/100")
    for l in limits:
        print(
            f"{l['limit']} | "
            f"{l['sessions_count']} | "
            f"{l['hands_count']} | "
            f"{round(l['total_result_money'], 2)} | "
            f"{round(l['total_result_bb'], 2)} | "
            f"{l['bb_per_100']}"
        )


# =========================
#        EXPORT
# =========================

def export_sessions_to_csv(sessions, filename):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "session_id", "start_time", "end_time", "duration_minutes",
            "hands_count", "limit", "total_result_money",
            "total_result_bb", "bb_per_100"
        ])

        for s in sessions:
            writer.writerow([
                s["session_id"],
                s["start_time"],
                s["end_time"],
                s["duration_minutes"],
                s["hands_count"],
                s["limit"],
                s["total_result_money"],
                s["total_result_bb"],
                s["bb_per_100"],
            ])


def export_full_to_json(summary, sessions, limits, filename):
    data = {
        "summary": summary,
        "sessions": sessions,
        "limits": limits,
    }
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


# =========================
#           MAIN
# =========================

def main():
    args = parse_args()

    base_path = Path(args.hands_path)
    if not base_path.exists():
        print(f"Ошибка: путь {base_path} не существует.", file=sys.stderr)
        sys.exit(1)

    # Читаем файлы
    all_raw_hands = []
    for file in iter_hand_files(base_path, args.recursive, args.all_files):
        try:
            text = file.read_text(encoding=args.encoding)
        except Exception as e:
            print(f"Ошибка чтения файла {file}: {e}", file=sys.stderr)
            continue

        blocks = split_raw_hands(text)
        all_raw_hands.extend(blocks)

    if not all_raw_hands:
        print("Раздачи не найдены.")
        sys.exit(0)

    # Парсим раздачи
    hands = []
    for raw in all_raw_hands:
        try:
            h = parse_hand(raw, args.hero_name)
            hands.append(h)
        except Exception as e:
            print(f"Ошибка парсинга раздачи: {e}", file=sys.stderr)

    # Формируем сессии
    sessions = build_sessions(hands, args.session_gap_minutes)

    # Статистика лимитов
    limits = build_limits_stats(sessions)

    # Сводка
    summary = build_summary(hands, sessions)

    # Вывод отчётов
    if "summary" in args.report:
        print_summary(summary)

    if "sessions" in args.report:
        print_sessions_report(sessions)

    if "limits" in args.report:
        print_limits_report(limits)

    # Экспорт CSV
    if args.export_csv:
        export_sessions_to_csv(sessions, args.export_csv)
        print(f"\nCSV экспортирован в {args.export_csv}")

    # Экспорт JSON
    if args.export_json:
        export_full_to_json(summary, sessions, limits, args.export_json)
        print(f"JSON экспортирован в {args.export_json}")


if __name__ == "__main__":
    main()