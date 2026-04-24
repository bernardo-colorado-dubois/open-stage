import logging
from openai import OpenAI
from open_stage.core.base_ai import BasePromptTransformer

logger = logging.getLogger(__name__)


class OpenAIPromptTransformer(BasePromptTransformer):

    def __init__(self, name: str, model: str, api_key: str, prompt: str, max_tokens: int = 16000) -> None:
        super().__init__(name, model, api_key, prompt, max_tokens)

    def _initialize_client(self) -> None:
        try:
            self.client = OpenAI(api_key=self.api_key)
            logger.info("OpenAIPromptTransformer '%s' client initialized (model=%s)", self.name, self.model)
        except Exception as e:
            raise ValueError(f"OpenAIPromptTransformer '{self.name}' failed to initialize client: {str(e)}")

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
