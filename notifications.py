# notifications.py

import os
from typing import Dict
from dotenv import load_dotenv
import requests
import pandas as pd

# Load environment variables from .env
load_dotenv()
GEMINI_API_KEY = os.getenv('GEMINI_API')
GEMINI_API_URL = os.getenv('GEMINI_API_URL')  # Replace with actual URL

def get_client_summary(client_id: int, data_dir: str = "case1") -> str:
    """
    Loads and summarizes client's transaction and transfer data for Gemini prompt.
    """
    df = pd.read_csv(f'{data_dir}/clients.csv', index_col='client_code')
    # print(df)
    client_row = df.loc[client_id]
    return client_row
    # import pandas as pd
    # transactions_path = os.path.join(data_dir, f"client_{client_id}_transactions_3m.csv")
    # transfers_path = os.path.join(data_dir, f"client_{client_id}_transfers_3m.csv")
    # summary = []
    # try:
    #     transactions_df = pd.read_csv(transactions_path)
    #     top_categories = transactions_df.groupby('category')['amount'].sum().nlargest(3)
    #     total_spend = transactions_df['amount'].sum()
    #     summary.append(f"Топ категории трат: {', '.join([f'{cat} ({amt:,.0f} KZT)' for cat, amt in top_categories.items()])}")
    #     summary.append(f"Общая сумма трат: {total_spend:,.0f} KZT")
    # except Exception as e:
    #     summary.append("Нет данных по транзакциям.")
    # try:
    #     transfers_df = pd.read_csv(transfers_path)
    #     transfer_types = transfers_df.groupby('type')['amount'].sum()
    #     top_transfers = transfer_types.nlargest(2)
    #     summary.append(f"Топ типы переводов: {', '.join([f'{typ} ({amt:,.0f} KZT)' for typ, amt in top_transfers.items()])}")
    # except Exception as e:
    #     summary.append("Нет данных по переводам.")
    # return "; ".join(summary)

# Placeholder for GEMINI framework integration

def generate_meta_prompt(product_name: str, value):
    if product_name.upper() in ['КАРТЫ ДЛЯ ПУТЕШЕСТВИЙ', 'ПРЕМИАЛЬНАЯ КАРТА', 'КРЕДИТНАЯ КАРТА']:
        return f"С {product_name} клиент получил бы кэшбеков {value} тенге"
    if 'депозит' in product_name.lower():
        temp_dep_dict = {
                'Депозит Мультивалютный' :'ставка 14.5%, пополнение и снятие без ограничений, Кому подходит: хранить/ребалансировать валюты с доступом к деньгам.',
                'Депозит Сберегательный': ' ставка 16,50%. пополнение — нет, снятие — нет (до конца срока).Кому подходит: максимальный доход при готовности «заморозить» средства.',
                'Депозит Накопительный': 'ставка 15.5%, пополнение — да, снятие — нет.Кому подходит: планомерно откладывать под повышенную ставку.'
            }
        return  temp_dep_dict[product_name]
    if product_name in ['Инвестиции','Золотые слитки', 'Обмен валют']:
        return f"На {product_name} клиент влил {value*100}% всех своих денег"
    

def generate_personalized_text(client_id: int, product_name: str, value) -> str:
    """
    Uses GEMINI framework to generate personalized notification text for the user.
    """
    client_summary = get_client_summary(client_id)
    prompt = (
        f"Клиенту с ID {client_id} рекомендуется продукт: {product_name}. "
        f"Данные клиента: {client_summary}. Мета-данные: {generate_meta_prompt(product_name, value)}"
        "Сгенерируй персонализированный текст пуш-уведомления на русском языке, объясняющий выгоды выбранного продукта с учетом этих данных. ориентир 180–220 символов для пушей"
    )

    if GEMINI_API_KEY and GEMINI_API_URL:
        try:
            response = requests.post(
                GEMINI_API_URL,
                json={
                    "contents": [
                        {
                            "parts": [
                                {"text": prompt}
                            ]
                        }
                    ]
                }
            )
            if response.status_code == 200:
                data = response.json()
                # Gemini returns the result in response.json()['candidates'][0]['content']['parts'][0]['text']
                return (
                    data.get('candidates', [{}])[0]
                        .get('content', {})
                        .get('parts', [{}])[0]
                        .get('text', '')
                )
            else:
                print(f"GEMINI API error: {response.status_code}")
                return f"GEMINI API error: {response.status_code}"
        except Exception as e:
            print(f"GEMINI API exception: {e}")
            return f"GEMINI API exception: {e}"

    return f"Уважаемый клиент! Мы рекомендуем вам {product_name} — это лучший выбор для вас по результатам анализа ваших операций. Ознакомьтесь с преимуществами прямо сейчас!"

def send_push_notification(client_id: int, product_name: str, scores: Dict[str, float]):
    """
    Sends a push notification to the user about the suggested product.
    """
    notification_text = generate_personalized_text(client_id, product_name, scores)

    print(f"[Push] Клиент {client_id}: {notification_text}")
    print({
        'client_code': client_id,
        'product': product_name,
        'push_notification': notification_text
    })
    return {
        'client_code': client_id,
        'product': product_name,
        'push_notification': notification_text
    }

# Example usage:
if __name__ == "__main__":
    # Example data
    client_id = 21
    product_name = "Золотые слитки"
    scores = {
        "Карта для путешествий": 12000.0,
        "Премиальная карта": 25000.0,
        "Кредитная карта": 8000.0
    }
    # print(get_client_summary(client_id))
    send_push_notification(client_id, product_name, '0.4')