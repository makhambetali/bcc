# def load_client_data(client_id: int, profiles_df: pd.DataFrame) -> Dict[str, Any]:
#     """
#     Загружает все данные (профиль, транзакции, переводы) для одного клиента по его ID.
#     """
#     print(f"Загрузка данных для клиента ID: {client_id}...")
#     try:
#         client_profile = profiles_df.loc[client_id]
#         transactions_path = os.path.join(DATA_DIRECTORY, f'client_{client_id}_transactions_3m.csv')
        

#         transfers_path = os.path.join(DATA_DIRECTORY, f'client_{client_id}_transfers_3m.csv')

#         transactions_df = pd.read_csv(transactions_path)
#         transfers_df = pd.read_csv(transfers_path)

#         print("Данные успешно загружены.")
#         return {
#             "profile": client_profile,
#             "transactions": transactions_df,
#             "transfers": transfers_df
#         }
#     except (FileNotFoundError, KeyError) as e:
#         print(f"ОШИБКА: Не удалось загрузить все данные для клиента {client_id}. {e}")
#         return {}



# def calculate_travel_card_cashback(client_data) -> float:
#     """
#     Рассчитывает долю расходов, релевантных для "Карты для путешествий",
#     от общей суммы трат клиента.

#     Args:
#         transactions_df: DataFrame с транзакциями клиента.

#     Returns:
#         float: Соотношение (доля) от 0.0 до 1.0.
#     """
#     transactions_df = client_data.get('transactions')
#     if transactions_df.empty:
#         return 0.0

#     total_spend = transactions_df['amount'].sum()
#     if total_spend == 0:
#         return 0.0

#     is_travel_category = transactions_df['category'].isin(['Такси', 'Путешествия', 'Отели'])
#     # is_fx_currency = transactions_df['currency'].isin(['USD', 'EUR'])
#     relevant_spend = transactions_df[is_travel_category]['amount'].sum()
#     # ratio = relevant_spend / total_spend
#     return relevant_spend * 0.04 #сколько денег можно вернуть кэшбеком


# def calculate_premium_card_cashback(client_data):
#     MAX_CASHBACK = 100_000 * 3
#     transactions_df = client_data.get('transactions')
#     transfers_df = client_data.get('transfers')
#     profile_df = client_data.get('profile')
#     avg_balance = int(profile_df['avg_monthly_balance_KZT'])
#     avg_balance_cashback = 0
#     if avg_balance < 1_000_000:
#         tier_cashback_rate = 0.02
#     elif 1_000_000 <= avg_balance and avg_balance < 6_000_000:
#         tier_cashback_rate = 0.03
#     else:
#         tier_cashback_rate = 0.04
#     avg_balance *= tier_cashback_rate
#     premium_expenses_cashback = 0.04 * transactions_df[transactions_df['category'].isin(['Ювелирные украшения', 'Косметика и Парфюмерия', 'Кафе и рестораны'])]['amount'].sum()
#     print(tier_cashback_rate)
#     return min(avg_balance + premium_expenses_cashback, MAX_CASHBACK)

# def calculate_credit_card_cashback(client_data):
#     transactions_df = client_data.get('transactions')
#     spend_by_category = transactions_df.groupby('category')['amount'].sum()
#     online_categories = {'Играем дома', 'Едим дома', 'Смотрим дома'}
#     top_3_categories = spend_by_category.nlargest(3)
#     # 4. Объединяем топ-3 и онлайн-категории в одно множество, чтобы избежать двойного подсчета
#     # all_relevant_categories = set(top_3_categories.index).union(online_categories)
#     all_relevant_categories = online_categories
#     # 5. Считаем итоговую сумму по уникальному списку категорий
#     final_sum = spend_by_category[spend_by_category.index.isin(all_relevant_categories)].sum()
    
#     print(final_sum, list(all_relevant_categories), top_3_categories)

#     return final_sum * 0.1

# def calculate_currency_exchange_cashback(client_data):
#     transactions_df = client_data.get('transfers')
#     if transactions_df.empty:
#         return 0.0

#     total_spend = transactions_df['amount'].sum()
#     if total_spend == 0:
#         return 0.0

#     is_travel_category = transactions_df['type'].isin(['fx_sell', 'fx_buy'])
#     # is_fx_currency = transactions_df['currency'].isin(['USD', 'EUR'])
#     relevant_spend = transactions_df[is_travel_category]['amount'].sum()
#     # ratio = relevant_spend / total_spend
#     return relevant_spend/total_spend #сколько денег можно вернуть кэшбеком




# def calculate_gold_cashback(client_data):
#     transactions_df = client_data.get('transfers')
#     if transactions_df.empty:
#         return 0.0

#     total_spend = transactions_df['amount'].sum()
#     if total_spend == 0:
#         return 0.0

#     is_travel_category = transactions_df['type'].isin(['gold_buy_out', 'gold_sell_in'])
#     # is_fx_currency = transactions_df['currency'].isin(['USD', 'EUR'])
#     relevant_spend = transactions_df[is_travel_category]['amount'].sum()
#     # ratio = relevant_spend / total_spend
#     return relevant_spend/total_spend #сколько денег можно вернуть кэшбеком

# def calculate_invest_cashback(client_data):
#     transactions_df = client_data.get('transfers')
#     if transactions_df.empty:
#         return 0.0

#     total_spend = transactions_df['amount'].sum()
#     if total_spend == 0:
#         return 0.0

#     is_travel_category = transactions_df['type'].isin(['invest_out', 'invest_in'])
#     # is_fx_currency = transactions_df['currency'].isin(['USD', 'EUR'])
#     relevant_spend = transactions_df[is_travel_category]['amount'].sum()
#     # ratio = relevant_spend / total_spend
#     return relevant_spend/total_spend #сколько денег можно вернуть кэшбеком

# if __name__ == "__main__":

#     try:
#         all_profiles = pd.read_csv(CLIENT_PROFILES_PATH).set_index('client_code')
#     except FileNotFoundError:
#         print(f"Критическая ошибка: Файл профилей {CLIENT_PROFILES_PATH} не найден.")
#         exit()
#     l = []
#     for i in range(1, 60):

#         CLIENT_TO_ANALYZE = i


#         client_raw_data = load_client_data(CLIENT_TO_ANALYZE, all_profiles)

#         if client_raw_data:
#             # client_transactions = client_raw_data.get('transactions')


#             travel_cashback = calculate_travel_card_cashback(client_raw_data)
#             premium_cashback = calculate_premium_card_cashback(client_raw_data)
#             credit_cashback = calculate_credit_card_cashback(client_raw_data)

#             # print("\n" + "="*40)
#             # print(f"РЕЗУЛЬТАТ АНАЛИЗА ДЛЯ Клиент ID: {CLIENT_TO_ANALYZE}:")
#             # print(f'КАРТЫ ДЛЯ ПУТЕШЕСТВИЙ: {travel_cashback} KZT')
#             # print(f'ПРЕМИАЛЬНАЯ КАРТА: {premium_cashback} KZT')
#             # print(f'КРЕДИТНАЯ КАРТА: {credit_cashback} KZT')
#             # print(calculate_currency_exchange_cashback(client_raw_data))
#             print(calculate_gold_cashback(client_raw_data))
#             # print(calculate_invest_cashback(client_raw_data))
#             l.append(calculate_currency_exchange_cashback(client_raw_data))
#             # if calculate_currency_exchange_cashback(client_raw_data) > 0.5:
#             #     print()
#             print("="*40)
#     print([el for el in l if el > 0])

