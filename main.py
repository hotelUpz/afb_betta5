# ============================================================
# FILE: main.py
# ROLE: Entry point
# ============================================================

import asyncio

from CORE.bot import FundingArbBot
from c_log import UnifiedLogger


def main() -> None:
    logger = UnifiedLogger(name="core", context="MAIN")
    try:
        bot = FundingArbBot(logger=logger)
        asyncio.run(bot.run_forever())
    except KeyboardInterrupt:
        logger.info("[APP] stopped by user")
    except Exception as e:
        logger.error(f"[APP] startup/runtime fatal: {e}")
        raise


if __name__ == "__main__":
    main()


# chmod 600 ssh_key.txt
# eval "$(ssh-agent -s)"
# ssh-add ssh_key.txt
# ssh -T git@github.com