import pandas as pd
import os
from typing import Dict, Any
from notifications import send_push_notification
import csv
from pathlib import Path

DATA_DIRECTORY = 'case1'
CLIENT_PROFILES_PATH = os.path.join(DATA_DIRECTORY, 'clients.csv')

def write_to_csv(row):
    out_path = Path("out/recommendations_append.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = out_path.exists()
    with out_path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["client_code", "product", "push_notification"])
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

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
            # print(self.client_profile)
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
        avg_balance = int(self.client_profile.get('avg_monthly_balance_KZT'))
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
    # def calculate_credit_card_cashback(self):
    #     """
    #     ИСПРАВЛЕНО: Считает кешбэк для Кредитной карты, выбирая топ-3
    #     категории только из заданного списка.
    #     """
    #     # Шаг 1: Определяем список категорий, из которых можно выбирать "любимые"
    #     eligible_categories = {
    #         'Косметика и Парфюмерия', 'Одежда и обувь', 'Медицина', 'Авто', 
    #         'Спорт', 'Развлечения', 'АЗС', 'Кино', 'Питомцы', 'Книги', 'Цветы'
    #     }
        
    #     # Шаг 2: Считаем траты клиента по всем категориям
    #     spend_by_category = self.transactions_df.groupby('category')['amount'].sum()

    #     # Шаг 3: Фильтруем траты, оставляя только те, что входят в разрешенный список
    #     eligible_spend = spend_by_category[spend_by_category.index.isin(eligible_categories)]
        
    #     # Шаг 4: Находим топ-3 самых затратных категории ИЗ ОТФИЛЬТРОВАННОГО СПИСКА
    #     top_3_from_eligible = eligible_spend.nlargest(3)

    #     # Шаг 5: Объединяем эти топ-3 категории с онлайн-сервисами, избегая дубликатов
    #     online_categories = {'Играем дома', 'Едим дома', 'Смотрим дома'}
    #     all_cashback_categories = set(top_3_from_eligible.index).union(online_categories)
        
    #     # Шаг 6: Считаем итоговую сумму трат, на которую будет начислен кешбэк
    #     total_cashback_spend = spend_by_category[spend_by_category.index.isin(all_cashback_categories)].sum()
        
    #     # Шаг 7: Возвращаем 10% от этой суммы
    #     return total_cashback_spend * 0.1

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
    
    def calculate_dep_savings_score(self):
        INTEREST_RATE = 0.165
        k = 0.8 # коэффицент строгости для is_stable_expenses: чем ниже - тем строже
        balance_stats = self.get_balance_statistics()
        is_high_balance = self.client_profile['avg_monthly_balance_KZT'] > balance_stats['mean']
        is_stable_expenses = self.transactions_df['amount'].std() < (self.transactions_df['amount'].mean() * k)

        # print(self.get_balance_statistics())
        # print(is_high_balance, is_stable_expenses)
        # print(self.client_profile['avg_monthly_balance_KZT'], balance_stats['mean'])

    def calc_liquidity_score(self):
        """
        Депозит Мультивалютный — оцениваем активность валютных операций.
        """
        INTEREST_RATE = 0.145
        if self.transfers_df.empty:
            return 0.0

        # fx_topups = self.transfers_df[self.transfers_df.type == "deposit_fx_topup_out"].amount.sum()
        # fx_withdraws = self.transfers_df[self.transfers_df.type == "deposit_fx_withdraw_in"].amount.sum()
        fx_ops = self.transfers_df[self.transfers_df.type.isin(["fx_buy", "fx_sell"])].shape[0]
        is_travel_category = self.transfers_df['type'].isin(['deposit_fx_topup_out', 'deposit_fx_withdraw_in'])
        # is_fx_currency = transactions_df['currency'].isin(['USD', 'EUR'])
        relevant_spend = self.transfers_df[is_travel_category]['amount'].sum()
        score = relevant_spend + fx_ops * 1000  # условный вес
        # return s
        # print(relevant_spend)
        # print('rs', score * (1 + INTEREST_RATE))
        return int(score * (1 + INTEREST_RATE))

    def calc_max_yield(self):
        """
        Депозит Сберегательный — проверяем один большой topup.
        """
        INTEREST_RATE = 0.165
        if self.transfers_df.empty:
            return 0.0
        is_savings = self.transfers_df['type'].isin(['deposit_topup_out'])
        savings_sum = self.transfers_df[is_savings]['amount'].sum()
        savings_count = self.transfers_df[is_savings].shape[0]
        balance_stats = self.get_balance_statistics()
        is_high_balance = self.client_profile['avg_monthly_balance_KZT'] > balance_stats['mean']
        # print('f', savings_count, savings_sum)

        return int(savings_count == 0 and is_high_balance)

    def calc_saving_discipline(self):
        """
        Депозит Накопительный — оцениваем регулярность пополнений.
        """
        INTEREST_RATE = 0.155
        if self.transfers_df.empty:
            return 0.0
        is_savings = self.transfers_df['type'].isin(['deposit_topup_out'])
        recurring_sum = self.transfers_df[is_savings].amount.sum()
        recurring_count = self.transfers_df[is_savings].shape[0]
        # print('r', recurring_count, recurring_sum)
        if recurring_count > 1:
            return int(recurring_sum * (1 + INTEREST_RATE) * (1 + 0.05 * recurring_count))
        return 0.0

    def choose_best_deposit(self):
        """
        Сравнивает три депозита и возвращает лучший вариант для клиента.
        """
        scores = {
            "Депозит Сберегательный": self.calc_max_yield(),
            "Депозит Накопительный": self.calc_saving_discipline(),
            "Депозит Мультивалютный": self.calc_liquidity_score(),
        }
        best_product = max(scores, key=scores.get)
        return {"scores": scores, "best_product": best_product}

    def execute(self):
        # print(f'currency exchange ratio: {self.calculate_currency_exchange_ratio()}')
        # print(f'gold ratio: {self.calculate_gold_ratio()}')
        # print(f'investment ratio: {self.calculate_invest_ratio()}')
        ratio_variables = {
            'Обмен валют': self.calculate_currency_exchange_ratio(),
            'Золотые слитки': self.calculate_gold_ratio(),
            'Инвестиции': self.calculate_invest_ratio()
        }
        
        # if max(ratio_variables.values()) > 0.3:
        max_key = 'Обмен валют'
        max_ratio = ratio_variables[max_key]
        
        for key in ratio_variables:
            if ratio_variables[key] > max_ratio:
                max_ratio = ratio_variables[key]
                max_key = key
        # print(f'MAX: {max_key}: {max_ratio}')
        if max_ratio > 0.3:
            # print(f'jb: {max_key}: {max_ratio}')
            print(max_key)
            row = send_push_notification(self.client_id,max_key, max_ratio)
            write_to_csv(row)
            return row
        
        # self.calculate_dep_savings_score()
        dep_result = self.choose_best_deposit()
        # print(list(dep_result['scores'].values()))
        max_score = dep_result['scores']['Депозит Сберегательный']
        max_score_key = 'Депозит Сберегательный'
        if list(dep_result['scores'].values()) != [0, 0, 0]:
            for key in dep_result['scores']:
                if dep_result['scores'][key] > max_score:
                    max_score = dep_result['scores'][key]
                    max_score_key = key
            print(max_score_key)
            row = send_push_notification(self.client_id,max_score_key, max_score)
            write_to_csv(row)
            return row
        travel_cashback = self.calculate_travel_card_cashback()
        premium_cashback = self.calculate_premium_card_cashback()
        credit_cashback = self.calculate_credit_card_cashback()
        print(f'КАРТЫ ДЛЯ ПУТЕШЕСТВИЙ: {travel_cashback} KZT')
        print(f'ПРЕМИАЛЬНАЯ КАРТА: {premium_cashback} KZT')
        print(f'КРЕДИТНАЯ КАРТА: {credit_cashback} KZT')
        # print(max(travel_cashback, premium_cashback))
        max_cashback = max(travel_cashback, premium_cashback)
        if max_cashback ==travel_cashback:
            print('КАРТЫ ДЛЯ ПУТЕШЕСТВИЙ')
            max_cashback_key = "КАРТЫ ДЛЯ ПУТЕШЕСТВИЙ"
        else:
            print('ПРЕМИАЛЬНАЯ КАРТА')
            max_cashback_key = "ПРЕМИАЛЬНАЯ КАРТА"
        row = send_push_notification(self.client_id,max_cashback_key, max_cashback)
        write_to_csv(row)
        return row
        


        # if dep_result.values() =
        # print("=== Депозиты ===")
        # print(dep_result['best_product'])
        # for k, v in dep_result["scores"].items():
        #     print(f"{k}: {v:.2f}")
        # print(f"Лучший депозит для клиента {self.client_id}: {dep_result['best_product']}")



    def get_balance_statistics(self) -> Dict[str, float]:
        """
        Рассчитывает ключевые статистические показатели (среднее, медиана, перцентили)
        по колонке avg_monthly_balance_KZT для всех клиентов.

        Args:
            profiles_df: DataFrame, содержащий профили всех клиентов.

        Returns:
            Словарь со статистическими показателями.
        """
        balance_data = self.all_profiles['avg_monthly_balance_KZT']
    
        stats = {
            "mean": balance_data.mean(),
            "median": balance_data.median(),
            "75%": balance_data.quantile(0.75),
            "85%": balance_data.quantile(0.85),
            "95%": balance_data.quantile(0.95)
        }
        return stats




if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Анализ клиента по ID")
    
    parser.add_argument("-id", "--client_id", type=int, help="ID клиента для анализа")
    
    args = parser.parse_args()

    if args.client_id:
        print(f"Запуск анализа для одного клиента с ID={args.client_id}")
        ca = ClientAnalyzer(args.client_id)
        ca.execute()
    else:
        print("ID клиента не указан. Запуск анализа для всех клиентов (1-60).")
        for id_to_analyze in range(1, 61):
            ca = ClientAnalyzer(id_to_analyze)
            ca.execute()