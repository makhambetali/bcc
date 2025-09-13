import pandas as pd
import os

# --- КОНФИГУРАЯ ---
DATA_DIRECTORY = 'case1'
CLIENT_PROFILES_PATH = os.path.join(DATA_DIRECTORY, 'clients.csv')

# --- ФУНКЦИИ СКОРИНГА (ОБНОВЛЕННЫЕ) ---

def score_travel_card(features: dict) -> float:
    travel_spend = (
        features['spend_by_category'].get('Путешествия', 0) +
        features['spend_by_category'].get('Такси', 0) +
        features['spend_by_category'].get('Отели', 0)
    )
    score = travel_spend * 0.04
    if features.get('fx_spend_sum', 0) > 0:
        score *= 1.2
    return score

def score_premium_card(features: dict) -> float:
    # ИЗМЕНЕНИЕ: Кешбэк теперь зависит от баланса (tier_cashback)
    avg_balance = features.get('avg_monthly_balance_KZT', 0)
    if avg_balance < 500000:
        tier_cashback_rate = 0.02
    elif avg_balance < 2000000:
        tier_cashback_rate = 0.03
    else:
        tier_cashback_rate = 0.04
        
    ATM_FEE = 500
    saved_fees = features.get('atm_withdrawal_count', 0) * ATM_FEE
    
    premium_spend = (
        features['spend_by_category'].get('Ювелирные украшения', 0) +
        features['spend_by_category'].get('Косметика и Парфюмерия', 0) +
        features['spend_by_category'].get('Рестораны', 0)
    )
    premium_cashback = premium_spend * 0.04
    
    total_spend = features['spend_by_category'].sum()
    base_spend = total_spend - premium_spend
    base_cashback = base_spend * tier_cashback_rate
    
    return saved_fees + premium_cashback + base_cashback

def score_credit_card(features: dict) -> float:
    top_3_spend = features.get('top_3_spend', 0)
    online_spend = (
        features['spend_by_category'].get('Едим дома', 0) +
        features['spend_by_category'].get('Смотрим дома', 0) +
        features['spend_by_category'].get('Играем дома', 0)
    )
    score = (top_3_spend + online_spend) * 0.10
    return score

def score_fx_exchange(features: dict) -> float:
    fx_volume = features.get('fx_buy_sum', 0) + features.get('fx_sell_sum', 0)
    return fx_volume * 0.005

def score_cash_loan(features: dict) -> float:
    if features.get('total_out', 0) > features.get('total_in', 0) * 1.2 and \
       features.get('loan_payment_count', 0) > 0:
        return 50000
    return 0
    
def score_savings_deposit(features: dict) -> float:
    # ИЗМЕНЕНИЕ: Оценка теперь основана на балансе
    # Сигнал: стабильный крупный остаток [cite: 43]
    avg_balance = features.get('avg_monthly_balance_KZT', 0)
    # Условие: остаток > 1 млн и он составляет > 50% от всех доходов (признак стабильности)
    if avg_balance > 1000000 and avg_balance > features.get('total_in', 0) * 0.5:
        # Выгода - примерный доход по вкладу за 3 месяца (ставка 14% годовых)
        return avg_balance * (0.14 / 4)
    return 0

def score_cumulative_deposit(features: dict) -> float:
    # ИЗМЕНЕНИЕ: Оценка теперь основана на балансе
    # Сигнал: регулярные небольшие остатки [cite: 46]
    avg_balance = features.get('avg_monthly_balance_KZT', 0)
    # Условие: остаток от 100 тыс до 1 млн (небольшой, но и не нулевой)
    if 100000 < avg_balance <= 1000000:
        # Выгода - примерный доход по вкладу за 3 месяца (ставка 12% годовых)
        return avg_balance * (0.12 / 4)
    return 0
    
def score_multicurrency_deposit(features: dict) -> float:
    # ИЗМЕНЕНИЕ: Оценка теперь основана на балансе и FX
    # Сигнал: свободный остаток + FX-активность [cite: 40]
    avg_balance = features.get('avg_monthly_balance_KZT', 0)
    has_fx_activity = features.get('fx_buy_sum', 0) > 0 or features.get('fx_spend_sum', 0) > 0
    if avg_balance > 200000 and has_fx_activity:
        return avg_balance * (0.10 / 4) # Ставка ниже, т.к. вклад гибкий
    return 0
    
def score_investments(features: dict) -> float:
    # ИЗМЕНЕНИЕ: Базовая оценка на основе баланса
    avg_balance = features.get('avg_monthly_balance_KZT', 0)
    if avg_balance > 500000:
        # Условный балл, т.к. прямую выгоду посчитать сложно
        return avg_balance * 0.05
    return 0

def score_gold_bars(features: dict) -> float:
    return features['spend_by_category'].get('Ювелирные украшения', 0) * 0.1

# --- ГЛАВНАЯ ФУНКЦИЯ АНАЛИЗА ---

def analyze_single_client(client_id: int, profiles_df: pd.DataFrame):
    print(f"--- Анализ для клиента ID: {client_id} ---")
    
    try:
        transactions_df = pd.read_csv(os.path.join(DATA_DIRECTORY, f'client_{client_id}_transactions_3m.csv'))
        transfers_df = pd.read_csv(os.path.join(DATA_DIRECTORY, f'client_{client_id}_transfers_3m.csv'))
        client_profile = profiles_df.loc[client_id]
    except (FileNotFoundError, KeyError):
        print(f"ОШИБКА: Файлы или профиль для клиента {client_id} не найдены.")
        return

    features = {}
    
    # ИЗМЕНЕНИЕ: Добавляем фичи из профиля
    features['avg_monthly_balance_KZT'] = client_profile['avg_monthly_balance_KZT']
    features['status'] = client_profile['status']
    
    # Фичи из транзакций
    features['spend_by_category'] = transactions_df.groupby('category')['amount'].sum()
    features['fx_spend_sum'] = transactions_df[transactions_df['currency'].isin(['USD', 'EUR'])]['amount'].sum()
    top_3 = features['spend_by_category'].nlargest(3)
    features['top_3_categories'] = top_3.index.tolist()
    features['top_3_spend'] = top_3.sum()
    
    # Фичи из переводов
    transfers_agg = transfers_df.groupby('type')['amount'].agg(['sum', 'size'])
    features['atm_withdrawal_count'] = transfers_agg.loc['atm_withdrawal']['size'] if 'atm_withdrawal' in transfers_agg.index else 0
    features['fx_buy_sum'] = transfers_agg.loc['fx_buy']['sum'] if 'fx_buy' in transfers_agg.index else 0
    features['loan_payment_count'] = transfers_agg.loc['loan_payment_out']['size'] if 'loan_payment_out' in transfers_agg.index else 0
    features['total_in'] = transfers_df[transfers_df['direction'] == 'in']['amount'].sum()
    features['total_out'] = transfers_df[transfers_df['direction'] == 'out']['amount'].sum()

    scores = {
        "Карта для путешествий": score_travel_card(features),
        "Премиальная карта": score_premium_card(features),
        "Кредитная карта": score_credit_card(features),
        "Обмен валют": score_fx_exchange(features),
        "Кредит наличными": score_cash_loan(features),
        "Депозит мультивалютный": score_multicurrency_deposit(features),
        "Депозит сберегательный": score_savings_deposit(features),
        "Депозит накопительный": score_cumulative_deposit(features),
        "Инвестиции": score_investments(features),
        "Золотые слитки": score_gold_bars(features)
    }
    
    # ИЗМЕНЕНИЕ: Корректировка на основе статуса
    if features['status'] == 'Студент':
        scores['Инвестиции'] *= 0.2
        scores['Золотые слитки'] *= 0.1
    elif features['status'] == 'Премиальный клиент':
        scores['Премиальная карта'] *= 1.5
        scores['Инвестиции'] *= 1.2

    print("\nОценки полезности продуктов (чем больше, тем лучше):")
    sorted_scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    for product, score in sorted_scores:
        print(f"  - {product}: {score:,.2f}")
    
    best_product = sorted_scores[0][0]
    print(f"\n✅ РЕКОМЕНДАЦИЯ: **{best_product}**\n")

# --- ЗАПУСК АНАЛИЗА ---
if __name__ == "__main__":
    try:
        all_profiles = pd.read_csv(CLIENT_PROFILES_PATH).set_index('client_code')
    except FileNotFoundError:
        print(f"ОШИБКА: Главный файл профилей {CLIENT_PROFILES_PATH} не найден.")
        exit()

    # 👇 УКАЖИТЕ ID КЛИЕНТА, КОТОРОГО НУЖНО ПРОАНАЛИЗИРОВАТЬ
    CLIENT_ID_TO_ANALYZE = 3
    
    analyze_single_client(CLIENT_ID_TO_ANALYZE, all_profiles)