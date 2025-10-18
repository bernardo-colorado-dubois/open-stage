from src.open_ai.chat_gpt import OpenAIPromptTransformer
from src.core.common import OpenOrigin, Printer, Pipe, Switcher
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

OPEN_AI_API_KEY = os.getenv("OPEN_AI_API_KEY")

# Create test data
df = pd.DataFrame({
    'id': [1, 2, 3, 4, 5, 6],
    'review': ['Great product!', 'Terrible experience.', 'It\'s okay.','Loved it!', 'Not worth the price.','I don\'t know.']
})

# Create pipeline
origin = OpenOrigin("test_data", df)

ai_transformer = OpenAIPromptTransformer(
    name="sentiment",
    model="gpt-3.5-turbo",  # Start with cheaper model
    api_key=OPEN_AI_API_KEY,
    prompt="Add sentiment column: positive, negative, or neutral",
    max_tokens=1000
)

sentiment_switcher = Switcher(
    name="sentiment_switcher",
    field="sentiment", 
    mapping={
        "positive": "positive_pipe",
        "negative": "negative_pipe",
        "neutral": "neutral_pipe"
    },
)

origin.add_output_pipe(Pipe("origin_pipe")).set_destination(ai_transformer)
ai_transformer.add_output_pipe(Pipe("transformed_data")).set_destination(sentiment_switcher)
sentiment_switcher.add_output_pipe(Pipe("positive_pipe")).set_destination(Printer("Positive Reviews"))
sentiment_switcher.add_output_pipe(Pipe("negative_pipe")).set_destination(Printer("Negative Reviews"))
sentiment_switcher.add_output_pipe(Pipe("neutral_pipe")).set_destination(Printer("Neutral Reviews"))

# Execute
origin.pump()