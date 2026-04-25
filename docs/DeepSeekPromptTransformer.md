# DeepSeekPromptTransformer - Usage Guide

Node that sends a DataFrame to a DeepSeek model and returns the transformed result as a new DataFrame. The entire cycle — DataFrame → CSV → LLM → CSV → DataFrame — is handled automatically.

---

## Features

- Sends the DataFrame as CSV to the model along with a natural language prompt
- Parses the model's CSV response back into a DataFrame
- `max_tokens` is capped at 8192 (DeepSeek API limit)
- Handles truncated responses by dropping the last incomplete row
- Strips markdown code blocks if the model returns them despite instructions

---

## Connectivity

```
1 input → 1 output
```

---

## Installation

```bash
pip install -e ".[deepseek]"
# or
pip install openai
```

---

## Basic Usage

### Example 1: Classify rows

```python
from open_stage.core.common import CSVOrigin, CSVDestination
from open_stage.deepseek.transformer import DeepSeekPromptTransformer
from open_stage.core.base import Pipe

origin = CSVOrigin("products", filepath_or_buffer="products.csv")

transformer = DeepSeekPromptTransformer(
    name="classifier",
    model="deepseek-chat",
    api_key="YOUR_DEEPSEEK_API_KEY",
    prompt="Add a column 'segment' classifying each product as 'budget', 'mid-range', or 'premium' based on the price column."
)

dest = CSVDestination("out", path_or_buf="classified.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(transformer)
transformer.add_output_pipe(Pipe("p2")).set_destination(dest)
origin.pump()
```

---

### Example 2: Translate a column

```python
transformer = DeepSeekPromptTransformer(
    name="translator",
    model="deepseek-chat",
    api_key="YOUR_DEEPSEEK_API_KEY",
    prompt="Translate the 'description' column from Spanish to English. Keep all other columns unchanged."
)
```

---

### Example 3: Enrich and normalize

```python
transformer = DeepSeekPromptTransformer(
    name="normalizer",
    model="deepseek-chat",
    api_key="YOUR_DEEPSEEK_API_KEY",
    prompt=(
        "Normalize the 'country' column to ISO 3166-1 alpha-2 codes. "
        "Add a 'region' column with values 'LATAM', 'EMEA', 'APAC', or 'NA'. "
        "Keep all other columns unchanged."
    )
)
```

---

### Example 4: Extract structured data from free text

```python
transformer = DeepSeekPromptTransformer(
    name="extractor",
    model="deepseek-chat",
    api_key="YOUR_DEEPSEEK_API_KEY",
    prompt=(
        "The 'notes' column contains free text. "
        "Extract from it two new columns: 'mentioned_product' and 'sentiment' (positive/negative/neutral). "
        "If the information is not present, use empty string."
    )
)
```

---

### Example 5: Control output size with max_tokens

```python
transformer = DeepSeekPromptTransformer(
    name="summarizer",
    model="deepseek-chat",
    api_key="YOUR_DEEPSEEK_API_KEY",
    prompt="Summarize the 'review' column into a single sentence and store it in a new 'summary' column.",
    max_tokens=4096
)
```

---

### Example 6: Load API key from environment

```python
import os

transformer = DeepSeekPromptTransformer(
    name="classifier",
    model="deepseek-chat",
    api_key=os.environ["DEEPSEEK_API_KEY"],
    prompt="Classify each row..."
)
```

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | Yes | — | Component name |
| `model` | str | Yes | — | DeepSeek model ID (e.g. `'deepseek-chat'`) |
| `api_key` | str | Yes | — | DeepSeek API key |
| `prompt` | str | Yes | — | Natural language instruction for the transformation |
| `max_tokens` | int | No | `8192` | Maximum tokens in the response — cannot exceed 8192 |

---

## How it works

1. Receives a DataFrame and serializes it to CSV
2. Sends the CSV + `prompt` to the model as the user message
3. A fixed system message instructs the model to always respond in raw CSV format
4. The response CSV is parsed back into a DataFrame and sent downstream

The model runs at `temperature=0.0` for deterministic output.

---

## Writing effective prompts

- Describe the **output** you want, not the steps to get there
- Reference column names explicitly
- Specify what to do with rows where the transformation doesn't apply
- Keep the DataFrame small for tasks that require reasoning on each row — cost and latency scale with input size

**Good prompt:**
```
Add a column 'risk_level' with values 'low', 'medium', or 'high' based on the 'amount' column:
low < 100, medium 100–1000, high > 1000. Keep all other columns unchanged.
```

**Avoid:**
```
Process the data and add some columns.
```

---

## Considerations

- **`max_tokens` cap**: DeepSeek enforces a hard limit of 8192 tokens per response — passing a higher value raises `ValueError` at construction time
- **Truncated responses**: if the model hits the token limit, the last (incomplete) row is automatically dropped and a warning is logged
- **Input size**: the entire DataFrame is sent as CSV in a single request — for large DataFrames, split rows first using `Filter` or `Switcher`
- **Output schema**: the response shape depends entirely on the prompt and the model — validate the output columns downstream if needed
- **API key**: never hardcode keys in source files — use environment variables or a secrets manager

---

## See Also

- [AnthropicPromptTransformer](./AnthropicPromptTransformer.md) - Same interface using Claude models
- [OpenAIPromptTransformer](./OpenAIPromptTransformer.md) - Same interface using OpenAI models
- [GeminiPromptTransformer](./GeminiPromptTransformer.md) - Same interface using Gemini models
- [Open-Stage Documentation](../README.md) - Complete documentation
