import pandas as pd
import os
from typing import Dict, Any

# --- ЧАСТЬ 1: КОНФИГУРАЦИЯ ---
DATA_DIRECTORY = 'case1'
CLIENT_PROFILES_PATH = os.path.join(DATA_DIRECTORY, 'clients.csv')


# --- ЧАСТЬ 2: АВТОМАТИЧЕСКИЙ РАСЧЁТ ПОРОГОВ ---
def calculate_global_thresholds(profiles_df: pd.DataFrame) -> dict:
    """
    Автоматически вычисляет пороговые значения по всей базе клиентов.
    """
    thresholds = {}
    balance_data = profiles_df['avg_monthly_balance_KZT']
    # Используем абсолютные значения из ТЗ для порогов баланса, а не перцентили
    thresholds['balance_mid'] = 1000000
    thresholds['balance_high'] = 6000000
    
    atm_counts = []
    for client_id in profiles_df.index:
        try:
            transfers_path = os.path.join(DATA_DIRECTORY, f'client_{client_id}_transfers_3m.csv')
            transfers_df = pd.read_csv(transfers_path)
            count = transfers_df[transfers_df['type'] == 'atm_withdrawal'].shape[0]
            atm_counts.append(count)
        except FileNotFoundError:
            atm_counts.append(0)
            
    thresholds['atm_frequency'] = pd.Series(atm_counts).quantile(0.75)
    
    print("--- Автоматически рассчитанные пороги (используются для бонусов) ---")
    print(f"  - Порог частых снятий: {thresholds['atm_frequency']:.0f} раз (75-й перцентиль)")
    print("-----------------------------------------------------------------")
    
    return thresholds


# --- ЧАСТЬ 3: ФУНКЦИЯ ЗАГРУЗКИ ДАННЫХ ---
def load_client_data(client_id: int, profiles_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Загружает все данные (профиль, транзакции, переводы) для одного клиента по его ID.
    """
    print(f"\nЗагрузка данных для клиента ID: {client_id}...")
    try:
        client_profile = profiles_df.loc[client_id]
        transactions_path = os.path.join(DATA_DIRECTORY, f'client_{client_id}_transactions_3m.csv')
        transfers_path = os.path.join(DATA_DIRECTORY, f'client_{client_id}_transfers_3m.csv')
        transactions_df = pd.read_csv(transactions_path)
        transfers_df = pd.read_csv(transfers_path)
        print("Данные успешно загружены.")
        return {"profile": client_profile, "transactions": transactions_df, "transfers": transfers_df}
    except (FileNotFoundError, KeyError) as e:
        print(f"ОШИБКА: Не удалось загрузить все данные для клиента {client_id}. {e}")
        return {}


# --- ЧАСТЬ 4: УНИВЕРСАЛЬНАЯ ФУНКЦИЯ ПОДГОТОВКИ ПРИЗНАКОВ ---
def engineer_features(client_data: dict) -> dict:
    """
    Извлекает из сырых данных все метрики, которые нужны для анализа.
    """
    features = {}
    profile = client_data.get("profile", pd.Series())
    transactions_df = client_data.get('transactions', pd.DataFrame())
    transfers_df = client_data.get('transfers', pd.DataFrame())

    # Передаем сырой DataFrame для сложных расчетов в скоринге
    features['raw_transfers'] = transfers_df

    features['avg_monthly_balance_KZT'] = profile.get('avg_monthly_balance_KZT', 0)

    if not transactions_df.empty:
        features['spend_by_category'] = transactions_df.groupby('category')['amount'].sum()
        top_3 = features['spend_by_category'].nlargest(3)
        features['top_3_categories'] = top_3.index.tolist()
    else:
        features['spend_by_category'] = pd.Series()
        features['top_3_categories'] = []

    if not transfers_df.empty:
        transfers_agg = transfers_df.groupby('type').size()
        features['atm_withdrawal_count'] = transfers_agg.get('atm_withdrawal', 0)
        features['p2p_out_count'] = transfers_agg.get('p2p_out', 0)
        repayment_types = {'installment_payment_out', 'cc_repayment_out'}
        features['has_repayment_history'] = any(t in transfers_agg.index for t in repayment_types)
    else:
        features['atm_withdrawal_count'] = 0
        features['p2p_out_count'] = 0
        features['has_repayment_history'] = False

    return features


# --- ЧАСТЬ 5: ФУНКЦИИ ОЦЕНКИ ПРОДУКТОВ (ОБНОВЛЕННЫЕ) ---

def score_travel_card(features: dict) -> float:
    """Логика обновлена согласно новым правилам."""
    CASHBACK_RATE = 0.04
    CASHBACK_LIMIT_3M = 90000
    PERKS_BONUS = 5000 # Бонус за привилегии (отели, аренда авто)

    travel_spend = (
        features['spend_by_category'].get('Путешествия', 0) +
        features['spend_by_category'].get('Такси', 0)
    )
    cashback_benefit = travel_spend * CASHBACK_RATE

    perks_benefit = 0
    if features['spend_by_category'].get('Отели', 0) > 0:
        perks_benefit = PERKS_BONUS
        
    return min(cashback_benefit, CASHBACK_LIMIT_3M) + perks_benefit

def score_premium_card(features: dict, thresholds: dict) -> float:
    """Логика полностью переработана согласно новым правилам."""
    ATM_FEE = 500
    CASHBACK_LIMIT_3M = 100000 * 3
    FREE_ATM_WITHDRAWAL_SUM_LIMIT_3M = 3000000 * 3

    transfers_df = features.get('raw_transfers', pd.DataFrame())
    atm_withdrawals = transfers_df[transfers_df['type'] == 'atm_withdrawal'].copy()
    saved_fees_atm = 0
    if not atm_withdrawals.empty:
        atm_withdrawals['cumulative_sum'] = atm_withdrawals['amount'].cumsum()
        free_withdrawals_df = atm_withdrawals[atm_withdrawals['cumulative_sum'] <= FREE_ATM_WITHDRAWAL_SUM_LIMIT_3M]
        saved_fees_atm = len(free_withdrawals_df) * ATM_FEE
        
    saved_fees_transfers = features.get('p2p_out_count', 0) * 150 # Переводы на карты РК бесплатны, убираем выгоду
    total_saved_fees = saved_fees_atm # + saved_fees_transfers

    avg_balance = features.get('avg_monthly_balance_KZT', 0)
    if avg_balance < 1000000: tier_cashback_rate = 0.02
    elif avg_balance < 6000000: tier_cashback_rate = 0.03
    else: tier_cashback_rate = 0.04
        
    premium_spend = features['spend_by_category'].get('Ювелирные изделия', 0) + features['spend_by_category'].get('Косметика и Парфюмерия', 0) + features['spend_by_category'].get('Рестораны', 0)
    premium_cashback = premium_spend * 0.04
    base_cashback = (features['spend_by_category'].sum() - premium_spend) * tier_cashback_rate
    capped_total_cashback = min(premium_cashback + base_cashback, CASHBACK_LIMIT_3M)

    return (total_saved_fees + capped_total_cashback)

def score_credit_card(features: dict) -> float:
    """Кешбэк до 10% в 3 «любимых категориях» + 10% на онлайн-услуги."""
    REPAYMENT_BONUS = 7500
    CASHBACK_RATE = 0.10
    
    top_categories = set(features.get('top_3_categories', []))
    online_categories = {'Едим дома', 'Смотрим дома', 'Играем дома'}
    all_cashback_categories = top_categories.union(online_categories)
    
    total_cashback_spend = sum(features['spend_by_category'].get(cat, 0) for cat in all_cashback_categories)
    cashback_benefit = total_cashback_spend * CASHBACK_RATE
    
    repayment_bonus = REPAYMENT_BONUS if features.get('has_repayment_history', False) else 0
        
    return cashback_benefit + repayment_bonus

# --- Функции-заглушки для остальных продуктов ---
def score_fx_exchange(features: dict) -> float: return 0
def score_cash_loan(features: dict) -> float: return 0
def score_multicurrency_deposit(features: dict) -> float: return 0
def score_savings_deposit(features: dict) -> float: return 0
def score_cumulative_deposit(features: dict) -> float: return 0
def score_investments(features: dict) -> float: return 0
def score_gold_bars(features: dict) -> float: return 0


# --- ЧАСТЬ 6: ГЛАВНЫЙ БЛОК ЗАПУСКА ---
if __name__ == "__main__":
    try:
        all_profiles = pd.read_csv(CLIENT_PROFILES_PATH).set_index('client_code')
    except FileNotFoundError:
        print(f"Критическая ошибка: Файл профилей {CLIENT_PROFILES_PATH} не найден.")
        exit()

    global_thresholds = calculate_global_thresholds(all_profiles)
    
    # 👇 УКАЖИТЕ ID КЛИЕНТА, КОТОРОГО НУЖНО ПРОАНАЛИЗИРОВАТЬ
    CLIENT_TO_ANALYZE = 21
    
    client_raw_data = load_client_data(CLIENT_TO_ANALYZE, all_profiles)

    if client_raw_data:
        client_features = engineer_features(client_raw_data)
        
        scores = {
            "Карта для путешествий": score_travel_card(client_features),
            "Премиальная карта": score_premium_card(client_features, global_thresholds),
            "Кредитная карта": score_credit_card(client_features),
            "Обмен валют": score_fx_exchange(client_features),
            "Кредит наличными": score_cash_loan(client_features),
            "Депозит мультивалютный": score_multicurrency_deposit(client_features),
            "Депозит сберегательный": score_savings_deposit(client_features),
            "Депозит накопительный": score_cumulative_deposit(client_features),
            "Инвестиции": score_investments(client_features),
            "Золотые слитки": score_gold_bars(client_features),
        }

        best_product = max(scores, key=scores.get)

        print("\n" + "="*40)
        print("РЕЗУЛЬТАТ АНАЛИЗА (по новым правилам):")
        print(f"  - Клиент ID: {CLIENT_TO_ANALYZE}")
        print("\nОценки продуктов:")
        for product, score in sorted(scores.items(), key=lambda item: item[1], reverse=True):
            print(f"  - {product}: {score:,.2f}")
        
        print(f"\n✅ РЕКОМЕНДАЦИЯ: **{best_product}**")
        print("="*40)