import os
from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')

import anthropic

client = anthropic.Anthropic(
  api_key=ANTHROPIC_API_KEY
)

message = client.messages.create(
  model="claude-sonnet-4-5-20250929",
  max_tokens=1024,
  messages=[
    {"role": "user", "content": "Hola, ¿cómo estás?"}
  ]
)

print(message.content)