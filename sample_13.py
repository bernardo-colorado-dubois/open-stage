from src.open_ai.chat_gpt import OpenAIPromptTransformer
from src.core.common import OpenOrigin, Printer, Pipe
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Create test data
df = pd.DataFrame({
    'id': [1, 2, 3],
    'review': ['Great product!', 'Terrible experience.', 'It\'s okay.']
})

# Create pipeline
origin = OpenOrigin("test_data", df)
transformer = OpenAIPromptTransformer(
    name="sentiment",
    model="gpt-3.5-turbo",  # Start with cheaper model
    api_key=os.getenv("OPEN_AI_API_KEY"),
    prompt="Add sentiment column: positive, negative, or neutral",
    max_tokens=1000
)
printer = Printer("output")

pipe1 = Pipe("p1")
pipe2 = Pipe("p2")

origin.add_output_pipe(pipe1).set_destination(transformer)
transformer.add_output_pipe(pipe2).set_destination(printer)

# Execute
origin.pump()