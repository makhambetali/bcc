# transfers_analysis.py
# Анализ переводов (transfers) по типам и группам для заданного клиента
# Требуется: pandas >= 1.3, numpy, matplotlib (опционально для графиков)

from __future__ import annotations
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional

# --- 1) Карта "тип -> группа" ---
CATEGORY_GROUPS: Dict[str, str] = {
    # Доходы / входящие поступления
    "salary_in": "incomes",
    "stipend_in": "incomes",
    "family_in": "incomes",
    "cashback_in": "incomes",
    "refund_in": "incomes",
    "card_in": "incomes",

    # Переводы и наличные
    "p2p_out": "p2p_and_cash",
    "card_out": "p2p_and_cash",
    "atm_withdrawal": "p2p_and_cash",

    # Регулярные расходы / обязательства
    "utilities_out": "obligations",
    "loan_payment_out": "obligations",
    "cc_repayment_out": "obligations",
    "installment_payment_out": "obligations",

    # Валюта (FX)
    "fx_buy": "fx",
    "fx_sell": "fx",

    # Инвестиции и сбережения
    "invest_out": "investments_and_savings",
    "invest_in": "investments_and_savings",
    "deposit_topup_out": "investments_and_savings",
    "deposit_fx_topup_out": "investments_and_savings",
    "deposit_fx_withdraw_in": "investments_and_savings",

    # Драгоценные металлы
    "gold_buy_out": "gold",
    "gold_sell_in": "gold",
}

# Полный ожидаемый перечень типов (на случай, если у клиента каких-то нет)
ALL_TYPES: List[str] = list(CATEGORY_GROUPS.keys())

# --- 2) Загрузка CSV для клиента ---
def load_client_transfers(client_id: int,
                          base_dir: str = "/mnt/data") -> pd.DataFrame:
    """
    Ожидает файлы формата: client_{id}_transfers_3m.csv
    Схема столбцов: date, type, direction, amount, currency, client_code
    """
    fname = os.path.join(base_dir, f"client_{client_id}_transfers_3m.csv")
    if not os.path.exists(fname):
        raise FileNotFoundError(f"Не найден файл: {fname}")

    df = pd.read_csv(fname)
    # Нормализация схемы
    req_cols = {"date", "type", "direction", "amount", "currency", "client_code"}
    missing = req_cols - set(df.columns)
    if missing:
        raise ValueError(f"В файле {fname} отсутствуют столбцы: {missing}")

    # Парсим даты и приводим к нужным типам
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["type"] = df["type"].astype(str)
    df["direction"] = df["direction"].astype(str)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["currency"] = df["currency"].astype(str)
    # Фильтр на всякий случай: внутри файла только нужный клиент
    # (некоторые датасеты бывают объединёнными)
    if "client_code" in df.columns:
        # оставим только строки, где client_code совпадает, если это число
        # если client_code строковый (например "3"), тоже сработает
        df = df[df["client_code"].astype(str) == str(client_id)]

    # Проставим группу по карте; неизвестные типы — "other"
    df["group"] = df["type"].map(CATEGORY_GROUPS).fillna("other")
    # Добавим year-month для помесячных срезов
    df["yyyymm"] = df["date"].dt.to_period("M").astype(str)

    return df

# --- 3) Агрегации ---
def aggregate_by_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Возвращает таблицу с суммой и числом операций по КАЖДОМУ типу за всё окно.
    Гарантированно включает все типы из ALL_TYPES (даже если у клиента = 0).
    """
    # Базовое агрегирование
    agg = (df.groupby("type")
             .agg(total_amount=("amount", "sum"),
                  n_ops=("amount", "size"))
             .reindex(ALL_TYPES)  # чтобы включить нулевые категории
             .fillna({"total_amount": 0.0, "n_ops": 0})
             .reset_index())

    # Доля от всех исходящих/входящих можно расчитать при желании:
    total = agg["total_amount"].sum()
    agg["share_of_total"] = np.where(total > 0, agg["total_amount"] / total, 0.0)
    return agg.sort_values("total_amount", ascending=False)

def aggregate_by_group(df: pd.DataFrame) -> pd.DataFrame:
    """
    Возвращает таблицу с суммой и числом операций по СМЫСЛОВЫМ группам.
    """
    agg = (df.groupby("group")
             .agg(total_amount=("amount", "sum"),
                  n_ops=("amount", "size"))
             .reset_index())
    total = agg["total_amount"].sum()
    agg["share_of_total"] = np.where(total > 0, agg["total_amount"] / total, 0.0)
    return agg.sort_values("total_amount", ascending=False)

def monthly_by_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Помесячная матрица: строки — месяцы (YYYY-MM), столбцы — типы, значения — суммы.
    Отсутствующие типы/месяцы заполняются нулями.
    """
    pivot = (df.pivot_table(index="yyyymm", columns="type", values="amount", aggfunc="sum")
               .reindex(columns=ALL_TYPES)  # фиксируем порядок/полный список столбцов
               .fillna(0.0))
    return pivot.sort_index()

def monthly_by_group(df: pd.DataFrame) -> pd.DataFrame:
    """
    Помесячно по группам (суммы).
    """
    pivot = (df.pivot_table(index="yyyymm", columns="group", values="amount", aggfunc="sum")
               .fillna(0.0))
    return pivot.sort_index()

# --- 4) Полезные метрики для скоринга/логики ---
def salary_regular(df: pd.DataFrame) -> Dict[str, float]:
    """
    Признаки по зарплате: наличие, кол-во месяцев с зарплатой, средняя/медианная сумма.
    """
    sdf = df[df["type"] == "salary_in"].copy()
    by_month = sdf.groupby("yyyymm")["amount"].sum()
    return {
        "has_salary": float(by_month.sum() > 0),
        "months_with_salary": int((by_month > 0).sum()),
        "salary_avg": float(sdf["amount"].mean() if not sdf.empty else 0.0),
        "salary_median": float(sdf["amount"].median() if not sdf.empty else 0.0),
    }

def debt_load(df: pd.DataFrame) -> Dict[str, float]:
    """
    Оценка долговой нагрузки по трансферам (без учёта транзакций).
    """
    loan = df.loc[df["type"] == "loan_payment_out", "amount"].sum()
    cc = df.loc[df["type"] == "cc_repayment_out", "amount"].sum()
    inst = df.loc[df["type"] == "installment_payment_out", "amount"].sum()
    return {
        "loan_payment_out_sum": float(loan),
        "cc_repayment_out_sum": float(cc),
        "installment_payment_out_sum": float(inst),
        "debt_sum": float(loan + cc + inst),
    }

def cash_and_p2p(df: pd.DataFrame) -> Dict[str, float]:
    """
    Профиль 'кэш/переводы': снятия и p2p/card_out.
    """
    atm = df.loc[df["type"] == "atm_withdrawal", "amount"].sum()
    p2p = df.loc[df["type"] == "p2p_out", "amount"].sum()
    c2c = df.loc[df["type"] == "card_out", "amount"].sum()
    total_out = df.loc[df["direction"].str.lower().eq("out"), "amount"].sum()
    share = (atm + p2p + c2c) / total_out if total_out > 0 else 0.0
    return {
        "atm_withdrawal_sum": float(atm),
        "p2p_out_sum": float(p2p),
        "card_out_sum": float(c2c),
        "cash_p2p_share_of_out": float(share),
        "total_out_amount": float(total_out),
    }

# --- 5) Главная функция анализа ---
def analyze_client_transfers(client_id: int,
                             base_dir: str = "/mnt/data",
                             show_examples: bool = True) -> Dict[str, pd.DataFrame | dict]:
    """
    Возвращает словарь с:
      - by_type         : агрегаты по типам
      - by_group        : агрегаты по группам
      - monthly_types   : по типам, помесячно
      - monthly_groups  : по группам, помесячно
      - features        : ключевые метрики (dict)
    """
    df = load_client_transfers(client_id, base_dir)

    out = {
        "by_type": aggregate_by_type(df),
        "by_group": aggregate_by_group(df),
        "monthly_types": monthly_by_type(df),
        "monthly_groups": monthly_by_group(df),
        "features": {
            **salary_regular(df),
            **debt_load(df),
            **cash_and_p2p(df),
        }
    }

    if show_examples:
        print("\n=== Аггрегаты по типам ===")
        print(out["by_type"].to_string(index=False))



      





    return out

# --- 6) (опционально) Быстрые графики ---
def plot_group_bars(agg_groups: pd.DataFrame, title: str = "Transfers by Groups"):
    """
    Простой bar chart по группам (если нужен на лету).
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib не установлен — график пропущен")
        return

    ax = agg_groups.sort_values("total_amount", ascending=False).plot(
        kind="bar", x="group", y="total_amount", legend=False, rot=30, figsize=(9, 4)
    )
    ax.set_ylabel("Сумма, ₸")
    ax.set_title(title)
    plt.tight_layout()
    plt.show()


def analyze_transfers_percent(client_id, base_dir="./"):
    # Загружаем файл трансферов
    df = pd.read_csv(f"{base_dir}/client_{client_id}_transfers_3m.csv")

    # Оставим только числовые суммы
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    # Можно ограничиться только исходящими (траты):
    df_out = df[df["direction"] == "out"]

    # Общая сумма
    total = df_out["amount"].sum()

    # Группировка по типам
    agg = (df_out.groupby("type")["amount"].sum()
           .reset_index()
           .sort_values("amount", ascending=False))

    # Добавим % от общего
    agg["percent"] = agg["amount"] / total * 100

    return agg






'''


ТРАНСФЕРЫ

'''
def load_client_transactions(client_id: int, base_dir: str = "/mnt/data") -> pd.DataFrame:
    fname = f"{base_dir}/client_{client_id}_transactions_3m.csv"
    df = pd.read_csv(fname)

    # Чистим данные
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    df["category"] = df["category"].astype(str)
    df["yyyymm"] = df["date"].dt.to_period("M").astype(str)
    return df

# --- Аггрегации по категориям ---
def aggregate_by_category(df: pd.DataFrame) -> pd.DataFrame:
    agg = (df.groupby("category")["amount"].sum()
             .reset_index()
             .sort_values("amount", ascending=False))
    total = agg["amount"].sum()
    agg["percent"] = (agg["amount"] / total * 100).round(2) if total > 0 else 0
    return agg

def monthly_by_category(df: pd.DataFrame) -> pd.DataFrame:
    pivot = (df.pivot_table(index="yyyymm", columns="category", values="amount", aggfunc="sum")
               .fillna(0.0))
    return pivot.sort_index()

# --- Красивый вывод таблицы ---
def pretty_print_table(df: pd.DataFrame, title: str):
    print(f"\n=== {title} ===")
    print(df.to_string(index=False, justify="left"))

# --- Основная функция ---
def analyze_client_transactions(client_id: int,
                                base_dir: str = "/mnt/data",
                                show_examples: bool = True) -> Dict[str, pd.DataFrame]:
    df = load_client_transactions(client_id, base_dir)

    agg = aggregate_by_category(df)
    monthly = monthly_by_category(df)

    out = {
        "by_category": agg,
        "monthly": monthly,
        "features": {
            "all_categories": agg.to_dict(orient="records"),
            "total_spend": float(df["amount"].sum()),
            "avg_monthly_spend": float(df.groupby("yyyymm")["amount"].sum().mean()),
            "top_categories": agg.head(3)["category"].tolist()
        }
    }

    if show_examples:
        pretty_df = agg.copy()
        pretty_df["amount"] = pretty_df["amount"].map("{:,.0f} ₸".format)
        pretty_df["percent"] = pretty_df["percent"].map("{:.2f} %".format)
        pretty_print_table(pretty_df, "Траты по категориям (за 3 мес.)")

     

        print("\n=== Ключевые метрики ===")
        print(f"Общий расход: {out['features']['total_spend']:.0f} ₸")
        print(f"Средний расход в месяц: {out['features']['avg_monthly_spend']:.0f} ₸")
        print(f"Топ-3 категории: {', '.join(out['features']['top_categories'])}")

    return out

# --- 7) Пример запуска ---
if __name__ == "__main__":
    # Укажи нужный client_id и при необходимости путь к данным


    # result = analyze_transfers_percent(60)



    CLIENT_ID = 31
    BASE_DIR = "case1/"

    result = analyze_client_transfers(CLIENT_ID, BASE_DIR, show_examples=True)
    analyze_client_transactions(CLIENT_ID, BASE_DIR, show_examples=True)

    

    # Если нужно — нарисовать бар по группам:
    # plot_group_bars(result["by_group"], title=f"Client {CLIENT_ID}: Transfers by Groups")
