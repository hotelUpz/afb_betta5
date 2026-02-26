# ============================================================
# FILE: TG/templates.py
# ROLE: Message templates for Telegram
# ============================================================

def format_opportunity(opp: dict) -> str:
    # Заглушка — под свою структуру opportunity
    lines = ["📌 Funding opportunity"]
    for k, v in opp.items():
        lines.append(f"{k}: {v}")
    return "\n".join(lines)
