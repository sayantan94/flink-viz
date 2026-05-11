from pydantic import BaseModel


class PageView(BaseModel):
    user_id: str
    product_id: str
    ts_ms: int
