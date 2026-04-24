import logging
from openai import OpenAI
from open_stage.core.base_ai import BasePromptTransformer

logger = logging.getLogger(__name__)


class DeepSeekPromptTransformer(BasePromptTransformer):

    _BASE_URL = "https://api.deepseek.com"
    _MAX_TOKENS_LIMIT = 8192

    def __init__(self, name: str, model: str, api_key: str, prompt: str, max_tokens: int = 8192) -> None:
        super().__init__(name, model, api_key, prompt, max_tokens)
        if max_tokens > self._MAX_TOKENS_LIMIT:
            raise ValueError(
                f"DeepSeekPromptTransformer '{self.name}': max_tokens must be between 1 and "
                f"{self._MAX_TOKENS_LIMIT}, got {max_tokens}"
            )

    def _initialize_client(self) -> None:
        try:
            self.client = OpenAI(api_key=self.api_key, base_url=self._BASE_URL)
            logger.info("DeepSeekPromptTransformer '%s' client initialized (model=%s)", self.name, self.model)
        except Exception as e:
            raise ValueError(f"DeepSeekPromptTransformer '{self.name}' failed to initialize client: {str(e)}")

    def _call_api(self, system_message: str, user_message: str) -> dict:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message},
            ],
            max_tokens=self.max_tokens,
            temperature=0.0,
            stream=False,
        )
        return {
            'response_text': response.choices[0].message.content,
            'truncated': response.choices[0].finish_reason == "length",
            'input_tokens': response.usage.prompt_tokens,
            'output_tokens': response.usage.completion_tokens,
        }
