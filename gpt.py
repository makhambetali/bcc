import os
import pandas as pd

CASE_DIR = "case1"
CLIENTS_CSV = os.path.join(CASE_DIR, "clients.csv")

FX_TYPES = {"fx_buy", "fx_sell"}                       # обмен валют
FX_DEPOSIT_TYPES = {"deposit_fx_topup_out", "deposit_fx_withdraw_in"}  # валютный вклад

TRAVEL_CATS = {"Такси", "Путешествия", "Отели"}        # на всякий плюс к сигналу
# (категории транзакций описаны в документе) 

def find_multicurrency_candidates(
    base_dir=CASE_DIR, fx_ratio_threshold=0.25, min_fx_ops=3, min_travel_share=0.10
):
    profiles = pd.read_csv(os.path.join(base_dir, "clients.csv"))
    candidates = []

    for cid in profiles["client_code"].tolist():
        t_path = os.path.join(base_dir, f"client_{cid}_transfers_3m.csv")
        x_path = os.path.join(base_dir, f"client_{cid}_transactions_3m.csv")
        if not (os.path.exists(t_path) and os.path.exists(x_path)):
            continue

        transfers = pd.read_csv(t_path)
        tx = pd.read_csv(x_path)

        # --- FX активность ---
        total_amt = transfers["amount"].abs().sum() or 0.0
        fx_mask = transfers["type"].isin(FX_TYPES)
        fx_ops_cnt = int(fx_mask.sum())
        fx_amt = transfers.loc[fx_mask, "amount"].abs().sum() if total_amt > 0 else 0.0
        fx_ratio = (fx_amt / total_amt) if total_amt > 0 else 0.0

        # --- Уже есть валютный вклад? ---
        has_fx_deposit = transfers["type"].isin(FX_DEPOSIT_TYPES).any()

        # --- Доп. сигнал: траты на поездки/такси/отели ---
        total_spend = tx["amount"].sum() or 0.0
        travel_spend = tx.loc[tx["category"].isin(TRAVEL_CATS), "amount"].sum() if total_spend > 0 else 0.0
        travel_share = (travel_spend / total_spend) if total_spend > 0 else 0.0

        # --- Критерии под мультивалютный депозит ---
        if (not has_fx_deposit) and fx_ops_cnt >= min_fx_ops and fx_ratio >= fx_ratio_threshold:
            # скоринг: приоритет объёму FX, затем количеству операций, затем travel
            score = fx_ratio * 100 + fx_ops_cnt * 2 + travel_share * 20
            candidates.append({
                "client_code": cid,
                "fx_ratio": round(fx_ratio, 4),
                "fx_ops": fx_ops_cnt,
                "travel_share": round(travel_share, 4),
                "score": round(score, 4),
            })

    return sorted(candidates, key=lambda r: r["score"], reverse=True)

if __name__ == "__main__":
    res = find_multicurrency_candidates()
    print("Кандидаты на мультивалютный депозит (топ-10):")
    for row in res[:10]:
        print(row)
