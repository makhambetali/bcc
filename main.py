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
    thresholds['balance_mid'] = balance_data.quantile(0.75)
    thresholds['balance_high'] = balance_data.quantile(0.85)
    
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
    
    print("--- Автоматически рассчитанные пороги ---")
    print(f"  - Порог баланса (средний): {thresholds['balance_mid']:,.0f} KZT (75-й перцентиль)")
    print(f"  - Порог баланса (высокий): {thresholds['balance_high']:,.0f} KZT (85-й перцентиль)")
    print(f"  - Порог частых снятий: {thresholds['atm_frequency']:.0f} раз (75-й перцентиль)")
    print("-----------------------------------------")
    
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

    # Признаки из профиля
    features['avg_monthly_balance_KZT'] = profile.get('avg_monthly_balance_KZT', 0)

    # Признаки из транзакций
    if not transactions_df.empty:
        features['spend_by_category'] = transactions_df.groupby('category')['amount'].sum()
        features['fx_spend_sum'] = transactions_df[transactions_df['currency'].isin(['USD', 'EUR'])]['amount'].sum()
    else:
        features['spend_by_category'] = pd.Series()
        features['fx_spend_sum'] = 0

    # Признаки из переводов
    if not transfers_df.empty:
        transfers_agg = transfers_df.groupby('type')['amount'].agg(['sum', 'size'])
        features['atm_withdrawal_count'] = transfers_agg.loc['atm_withdrawal']['size'] if 'atm_withdrawal' in transfers_agg.index else 0
        features['p2p_out_count'] = transfers_agg.loc['p2p_out']['size'] if 'p2p_out' in transfers_agg.index else 0
    else:
        features['atm_withdrawal_count'] = 0
        features['p2p_out_count'] = 0

    return features


# --- ЧАСТЬ 5: ФУНКЦИИ ОЦЕНКИ ПРОДУКТОВ ---
def score_travel_card(features: dict) -> float:
    CASHBACK_RATE = 0.04
    CASHBACK_LIMIT_3M = 90000
    travel_spend = (
        features['spend_by_category'].get('Путешествия', 0) +
        features['spend_by_category'].get('Такси', 0) +
        features['spend_by_category'].get('Отели', 0)
    )
    base_benefit = travel_spend * CASHBACK_RATE
    if features.get('fx_spend_sum', 0) > 0:
        base_benefit *= 1.2
    return min(base_benefit, CASHBACK_LIMIT_3M)

def score_premium_card(features: dict, thresholds: dict) -> float:
    """
    [cite_start]Рассчитывает оценку (benefit) для продукта "Премиальная карта". [cite: 25]
    """
    BALANCE_THRESHOLD_MID = thresholds['balance_mid']
    BALANCE_THRESHOLD_HIGH = thresholds['balance_high']
    ATM_FREQUENCY_THRESHOLD = thresholds['atm_frequency']
    
    avg_balance = features.get('avg_monthly_balance_KZT', 0)
    if avg_balance < BALANCE_THRESHOLD_MID:
        tier_cashback_rate = 0.02
    elif avg_balance < BALANCE_THRESHOLD_HIGH:
        tier_cashback_rate = 0.03
    else:
        tier_cashback_rate = 0.04
        
    ATM_FEE = 500
    TRANSFER_FEE = 150
    CASHBACK_LIMIT_3M = 100000
    
    saved_fees = (features.get('atm_withdrawal_count', 0) * ATM_FEE) + (features.get('p2p_out_count', 0) * TRANSFER_FEE) #
    
    frequent_user_bonus = 0
    if features.get('atm_withdrawal_count', 0) >= ATM_FREQUENCY_THRESHOLD: #
        frequent_user_bonus = 5000
    
    premium_spend = (
        features['spend_by_category'].get('Ювелирные украшения', 0) +
        features['spend_by_category'].get('Косметика и Парфюмерия', 0) +
        features['spend_by_category'].get('Кафе и рестораны', 0)
    ) #
    premium_cashback = premium_spend * 0.04

    total_spend = features['spend_by_category'].sum()
    base_spend = total_spend - premium_spend
    base_cashback = base_spend * tier_cashback_rate

    total_cashback = premium_cashback + base_cashback
    capped_total_cashback = min(total_cashback, CASHBACK_LIMIT_3M) #
    print(saved_fees, capped_total_cashback, frequent_user_bonus)
    # return saved_fees + capped_total_cashback + frequent_user_bonus
    final_benefit = saved_fees + capped_total_cashback + frequent_user_bonus

    # Если баланс клиента ниже среднего, уменьшаем привлекательность премиум-продукта
    if features.get('avg_monthly_balance_KZT', 0) < thresholds['balance_mid']:
        final_benefit *= 0.2 # Штраф в 50%

    return final_benefit


# --- ЧАСТЬ 6: ГЛАВНЫЙ БЛОК ЗАПУСКА ---
if __name__ == "__main__":
    try:
        all_profiles = pd.read_csv(CLIENT_PROFILES_PATH).set_index('client_code')
    except FileNotFoundError:
        print(f"Критическая ошибка: Файл профилей {CLIENT_PROFILES_PATH} не найден.")
        exit()

    # Шаг 1: Автоматически вычисляем глобальные пороги
    global_thresholds = calculate_global_thresholds(all_profiles)
    CLIENT_TO_ANALYZE = 41
    
    # Шаг 2: Загружаем все сырые данные для клиента
    client_raw_data = load_client_data(CLIENT_TO_ANALYZE, all_profiles)

    if client_raw_data:
        # Шаг 3: Извлекаем из сырых данных все нужные фичи
        client_features = engineer_features(client_raw_data)

        # Шаг 4: Рассчитываем оценки для каждого продукта
        scores = {
            "Карта для путешествий": score_travel_card(client_features),
            "Премиальная карта": score_premium_card(client_features, global_thresholds),
        }

        # Шаг 5: Находим лучший продукт и выводим результаты
        best_product = max(scores, key=scores.get)

        print("\n" + "="*40)
        print("РЕЗУЛЬТАТ АНАЛИЗА:")
        print(f"  - Клиент ID: {CLIENT_TO_ANALYZE}")
        print("\nОценки продуктов:")
        for product, score in scores.items():
            print(f"  - {product}: {score:,.2f}")
        
        print(f"\n✅ РЕКОМЕНДАЦИЯ: **{best_product}**")
        print("="*40)