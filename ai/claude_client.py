import os

import anthropic
from dotenv import load_dotenv

from pipeline.config import BASE_DIR

load_dotenv(os.path.join(BASE_DIR, ".env"))

_MODEL = "claude-haiku-4-5-20251001"


def ask_claude(system_prompt, user_prompt, max_tokens=500):
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    response = client.messages.create(
        model=_MODEL,
        max_tokens=max_tokens,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return response.content[0].text
