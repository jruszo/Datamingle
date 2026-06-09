import logging
import os

from openai import OpenAI
from django.template import Context, Template

logger = logging.getLogger("default")

DEFAULT_OPENAI_MODEL = "gpt-4o-mini"


def get_openai_config():
    return {
        "api_key": os.environ.get("OPENAI_KEY", "").strip(),
        "base_url": os.environ.get("OPENAI_BASE_URL", "").strip(),
        "model": os.environ.get("OPENAI_MODEL", DEFAULT_OPENAI_MODEL).strip()
        or DEFAULT_OPENAI_MODEL,
    }


class OpenaiClient:
    def __init__(self):
        config = get_openai_config()
        self.base_url = config["base_url"]
        self.api_key = config["api_key"]
        self.default_chat_model = config["model"]
        self.default_query_template = os.environ.get(
            "OPENAI_QUERY_TEMPLATE", ""
        ).strip() or (
            "You are an engineer familiar with {{db_type}}. "
            "I will provide context and requirements. Generate a usable query only. "
            "Do not return comments or numbering. Return only the query statement: "
            "{{table_schema}} \n {{user_input}}"
        )
        self.client = OpenAI(base_url=self.base_url or None, api_key=self.api_key)

    def request_chat_completion(self, messages, **kwargs):
        """chat_completion"""
        completion = self.client.chat.completions.create(
            model=self.default_chat_model, messages=messages, **kwargs
        )
        return completion

    def generate_sql_by_openai(self, db_type: str, table_schema: str, user_input: str):
        """Generate a query from the provided context."""
        template = Template(self.default_query_template)
        current_context = Context(
            dict(db_type=db_type, table_schema=table_schema, user_input=user_input)
        )
        messages = [dict(role="user", content=template.render(current_context))]
        logger.info(messages)
        try:
            res = self.request_chat_completion(messages)
            return res.choices[0].message.content
        except Exception as e:
            raise ValueError(f"Failed to generate query with OpenAI: {e}")


def check_openai_config():
    """Validate whether required OpenAI API config exists."""
    return bool(get_openai_config()["api_key"])
