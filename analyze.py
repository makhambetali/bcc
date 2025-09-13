import os
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict

def analyze_and_plot(directory: str, top_n: int = 20):
    """
    Анализирует транзакции, подсчитывая количество и сумму операций по категориям,
    и строит два графика: по количеству и по сумме.

    :param directory: Путь к папке с файлами.
    :param top_n: Количество топовых категорий для отображения.
    """
    # Используем defaultdict для удобного суммирования
    category_sums = defaultdict(float)
    category_counts = defaultdict(int)

    if not os.path.isdir(directory):
        print(f"Ошибка: Директория '{directory}' не найдена.")
        return

    print(f"Анализ файлов в '{directory}'...")
    # --- Сбор и агрегация данных ---
    for filename in os.listdir(directory):
        if 'transfers' in filename.lower() or not filename.endswith('.csv'):
            continue

        file_path = os.path.join(directory, filename)
        try:
            df = pd.read_csv(file_path)
            
            # Проверяем наличие нужных столбцов
            if 'category' in df.columns and 'amount' in df.columns:
                # Убираем строки с пустыми значениями в ключевых столбцах
                df.dropna(subset=['category', 'amount'], inplace=True)
                
                # Группируем данные по категориям и считаем сумму и количество
                # 'sum' - общая сумма, 'size' - количество транзакций
                agg_data = df.groupby('category')['amount'].agg(['sum', 'size'])
                
                # Обновляем общие словари
                for category_id, data in agg_data.iterrows():
                    category_sums[category_id] += data['sum']
                    category_counts[category_id] += data['size']
                    
        except Exception as e:
            print(f"Не удалось обработать файл {filename}. Ошибка: {e}")

    if not category_counts:
        print("Не найдено данных для анализа.")
        return

    # --- Построение графиков ---
    # 1. График по КОЛИЧЕСТВУ операций
    # Сортируем категории по количеству
    top_counts = sorted(category_counts.items(), key=lambda item: item[1], reverse=True)[:top_n]
    create_plot(
        data=top_counts,
        title=f'Топ-{top_n} категорий по КОЛИЧЕСТВУ операций',
        ylabel='Количество операций',
        filename='plot_by_count.png'
    )

    # 2. График по СУММЕ операций
    # Сортируем категории по сумме
    top_sums = sorted(category_sums.items(), key=lambda item: item[1], reverse=True)[:top_n]
    create_plot(
        data=top_sums,
        title=f'Топ-{top_n} категорий по СУММЕ трат (KZT)',
        ylabel='Общая сумма, KZT',
        filename='plot_by_sum.png'
    )


def create_plot(data: list, title: str, ylabel: str, filename: str):
    """Вспомогательная функция для создания и сохранения графика."""
    if not data:
        print(f"Нет данных для построения графика '{title}'")
        return

    # Разделяем данные на метки и значения
    labels = [str(item[0]) for item in data]
    values = [item[1] for item in data]

    print(f"Построение графика: {title}")
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.figure(figsize=(12, 8))
    
    bars = plt.bar(labels, values, color='#4c72b0')
    
    plt.xlabel('ID Категории', fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.title(title, fontsize=16, fontweight='bold')
    plt.xticks(rotation=45, ha='right')
    
    # Форматируем значения для лучшей читаемости
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2.0, yval, f'{yval:,.0f}', va='bottom', ha='center')

    plt.tight_layout()
    plt.savefig(filename)
    print(f"График успешно сохранен в файл: {filename}")
    plt.close() # Закрываем фигуру, чтобы графики не смешивались


# --- Основная часть скрипта ---
if __name__ == "__main__":
    transactions_directory = 'case1'
    analyze_and_plot(transactions_directory, top_n=20)