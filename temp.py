import pandas as pd
import os
import numpy as np

# --- КОНФИГУРАЦИЯ ---
DATA_DIRECTORY = 'case1'
CLIENT_PROFILES_PATH = os.path.join(DATA_DIRECTORY, 'clients.csv')

def calculate_data_driven_thresholds():
    """
    Сканирует все данные по клиентам для расчета средних и перцентильных
    значений для баланса и частоты снятия наличных.
    """
    print("--- Расчёт пороговых значений на основе данных 60 клиентов ---")
    
    # --- 1. Анализ среднемесячного баланса ---
    try:
        profiles_df = pd.read_csv(CLIENT_PROFILES_PATH)
        balance_data = profiles_df['avg_monthly_balance_KZT']
        
        print("\n# Статистика по Среднемесячному балансу (avg_monthly_balance_KZT):")
        print(f"  - Среднее значение (mean): {balance_data.mean():,.0f} KZT")
        print(f"  - Медиана (50-й перцентиль): {balance_data.median():,.0f} KZT")
        print(f"  - 75-й перцентиль: {balance_data.quantile(0.75):,.0f} KZT")
        print(f"  - 85-й перцентиль: {balance_data.quantile(0.85):,.0f} KZT")
        print(f"  - 95-й перцентиль: {balance_data.quantile(0.95):,.0f} KZT")

    except FileNotFoundError:
        print(f"\nОШИБКА: Файл профилей {CLIENT_PROFILES_PATH} не найден.")
        return

    # --- 2. Анализ частоты снятий в банкоматах ---
    atm_withdrawal_counts = []
    
    for client_id in range(1, 61):
        try:
            transfers_path = os.path.join(DATA_DIRECTORY, f'client_{client_id}_transfers_3m.csv')
            transfers_df = pd.read_csv(transfers_path)
            
            # Считаем количество операций типа 'atm_withdrawal'
            count = transfers_df[transfers_df['type'] == 'atm_withdrawal'].shape[0]
            atm_withdrawal_counts.append(count)
            
        except FileNotFoundError:
            # Если файла нет, считаем, что снятий не было
            atm_withdrawal_counts.append(0)

    atm_counts_series = pd.Series(atm_withdrawal_counts)
    
    print("\n# Статистика по Частоте снятий в банкомате (за 3 месяца):")
    print(f"  - Среднее количество (mean): {atm_counts_series.mean():.1f} раз")
    print(f"  - Медиана (50-й перцентиль): {atm_counts_series.median():.0f} раз")
    print(f"  - 75-й перцентиль: {atm_counts_series.quantile(0.75):.0f} раз")
    print(f"  - 85-й перцентиль: {atm_counts_series.quantile(0.85):.0f} раз")
    print(f"  - 95-й перцентиль: {atm_counts_series.quantile(0.95):.0f} раз")


# --- Запуск анализа ---
if __name__ == "__main__":
    calculate_data_driven_thresholds()