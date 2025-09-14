import pandas as pd
import os
from typing import Dict, Any

DATA_DIRECTORY = 'case1'
CLIENT_PROFILES_PATH = os.path.join(DATA_DIRECTORY, 'clients.csv')

class ClientAnalyzer:
    def __init__(self, client_id):
        self.DATA_DIRECTORY = 'case1'
        self.CLIENT_PROFILES_PATH = os.path.join(self.DATA_DIRECTORY, 'clients.csv')
        self.client_id = client_id
        try:
            self.all_profiles = pd.read_csv(self.CLIENT_PROFILES_PATH).set_index('client_code')
        except FileNotFoundError:
            print(f"Критическая ошибка: Файл профилей {self.CLIENT_PROFILES_PATH} не найден.")
            exit()
        
        self.load_client_data()
    def load_client_data(self) -> Dict[str, Any]:
        """
        Загружает все данные (профиль, транзакции, переводы) для одного клиента по его ID.
        """
        print(f"Загрузка данных для клиента ID: {self.client_id}...")
        try:
            self.client_profile = self.all_profiles.loc[self.client_id]
            transactions_path = os.path.join(self.DATA_DIRECTORY, f'client_{self.client_id}_transactions_3m.csv')
            

            transfers_path = os.path.join(self.DATA_DIRECTORY, f'client_{self.client_id}_transfers_3m.csv')

            self.transactions_df = pd.read_csv(transactions_path)
            self.transfers_df = pd.read_csv(transfers_path)

            print("Данные успешно загружены.\n")
            
        except (FileNotFoundError, KeyError) as e:
            print(f"ОШИБКА: Не удалось загрузить все данные для клиента {self.client_id}. {e}")
            return {}
        
    def calculate_travel_card_cashback(self) -> float:
        """
        Рассчитывает долю расходов, релевантных для "Карты для путешествий",
        от общей суммы трат клиента.

        Args:
            transactions_df: DataFrame с транзакциями клиента.

        Returns:
            float: Соотношение (доля) от 0.0 до 1.0.
        """
        if self.transactions_df.empty:
            return 0.0

        total_spend = self.transactions_df['amount'].sum()
        if total_spend == 0:
            return 0.0

        is_travel_category = self.transactions_df['category'].isin(['Такси', 'Путешествия', 'Отели'])
        # is_fx_currency = transactions_df['currency'].isin(['USD', 'EUR'])
        relevant_spend = self.transactions_df[is_travel_category]['amount'].sum()
        # ratio = relevant_spend / total_spend
        return relevant_spend * 0.04 #сколько денег можно вернуть кэшбеком


    def calculate_premium_card_cashback(self):
        MAX_CASHBACK = 100_000 * 3
        profile_df = self.client_profile.get('profile')
        avg_balance = int(profile_df['avg_monthly_balance_KZT'])
        avg_balance_cashback = 0
        if avg_balance < 1_000_000:
            tier_cashback_rate = 0.02
        elif 1_000_000 <= avg_balance and avg_balance < 6_000_000:
            tier_cashback_rate = 0.03
        else:
            tier_cashback_rate = 0.04
        avg_balance *= tier_cashback_rate
        premium_expenses_cashback = 0.04 * self.transactions_df[self.transactions_df['category'].isin(['Ювелирные украшения', 'Косметика и Парфюмерия', 'Кафе и рестораны'])]['amount'].sum()
        # print(tier_cashback_rate)
        return min(avg_balance + premium_expenses_cashback, MAX_CASHBACK)

    def calculate_credit_card_cashback(self):

        spend_by_category = self.transactions_df.groupby('category')['amount'].sum()
        online_categories = {'Играем дома', 'Едим дома', 'Смотрим дома'}
        top_3_categories = spend_by_category.nlargest(3)
        # 4. Объединяем топ-3 и онлайн-категории в одно множество, чтобы избежать двойного подсчета
        # all_relevant_categories = set(top_3_categories.index).union(online_categories)
        all_relevant_categories = online_categories
        # 5. Считаем итоговую сумму по уникальному списку категорий
        final_sum = spend_by_category[spend_by_category.index.isin(all_relevant_categories)].sum()
        
        # print(final_sum, list(all_relevant_categories), top_3_categories)

        return final_sum * 0.1

    def calculate_currency_exchange_ratio(self):
        if self.transfers_df.empty:
            return 0.0

        total_spend = self.transfers_df['amount'].sum()
        if total_spend == 0:
            return 0.0

        is_travel_category = self.transfers_df['type'].isin(['fx_sell', 'fx_buy'])
        # is_fx_currency = transactions_df['currency'].isin(['USD', 'EUR'])
        relevant_spend = self.transfers_df[is_travel_category]['amount'].sum()
        # ratio = relevant_spend / total_spend
        return relevant_spend/total_spend #сколько денег можно вернуть кэшбеком




    def calculate_gold_ratio(self):
        if self.transfers_df.empty:
            return 0.0

        total_spend = self.transfers_df['amount'].sum()
        if total_spend == 0:
            return 0.0

        is_travel_category = self.transfers_df['type'].isin(['gold_buy_out', 'gold_sell_in'])
        # is_fx_currency = transactions_df['currency'].isin(['USD', 'EUR'])
        relevant_spend = self.transfers_df[is_travel_category]['amount'].sum()
        # ratio = relevant_spend / total_spend
        return relevant_spend/total_spend #сколько денег можно вернуть кэшбеком

    def calculate_invest_ratio(self):
        if self.transfers_df.empty:
            return 0.0

        total_spend = self.transfers_df['amount'].sum()
        if total_spend == 0:
            return 0.0

        is_travel_category = self.transfers_df['type'].isin(['invest_out', 'invest_in'])
        # is_fx_currency = transactions_df['currency'].isin(['USD', 'EUR'])
        relevant_spend = self.transfers_df[is_travel_category]['amount'].sum()
        # ratio = relevant_spend / total_spend
        return relevant_spend/total_spend #сколько денег можно вернуть кэшбеком

    def execute(self):
        print(f'currency exchange ratio: {self.calculate_currency_exchange_ratio()}')
        print(f'gold ratio: {self.calculate_gold_ratio()}')
        print(f'investment ratio: {self.calculate_invest_ratio()}')




if __name__ == '__main__':
    ID_TO_ANALYZE = 49
    ca = ClientAnalyzer(ID_TO_ANALYZE)
    ca.execute()