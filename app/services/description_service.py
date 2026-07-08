from __future__ import annotations

import html
import re
from dataclasses import dataclass

from app.services.settings import DescriptionSettings


@dataclass
class DescriptionResult:
    text: str
    source: str


class DescriptionService:
    """Fetch missing descriptions in the configured order."""

    def __init__(self, settings: DescriptionSettings):
        self.settings = settings

    async def fetch(self, tconst: str, title: str, year: int | None, title_type: str | None) -> DescriptionResult | None:
        if self.settings.tmdb_enabled:
            result = await self.fetch_from_tmdb(title, year, title_type)
            if result:
                return result

        if self.settings.imdb_scrape_enabled:
            result = await self.fetch_from_imdb(tconst)
            if result:
                return result

        return None

    async def fetch_from_tmdb(self, title: str, year: int | None, title_type: str | None) -> DescriptionResult | None:
        # Implemented in the browser phase. Kept async so network work stays off the UI thread.
        return None

    async def fetch_from_imdb(self, tconst: str) -> DescriptionResult | None:
        from playwright.async_api import async_playwright

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            try:
                page = await browser.new_page(
                    viewport={"width": 1366, "height": 1200},
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                )
                await page.goto(f"https://www.imdb.com/title/{tconst}/", wait_until="domcontentloaded", timeout=45000)
                await page.wait_for_timeout(2500)
                text = await self.extract_rendered_imdb_plot(page)
                if not text:
                    text = self.extract_imdb_plot(await page.content())
                return DescriptionResult(text=text, source="IMDb") if text else None
            finally:
                await browser.close()

    async def extract_rendered_imdb_plot(self, page) -> str:
        selectors = [
            '[data-testid="plot-xl"]',
            '[data-testid="plot-l"]',
            '[data-testid="plot-xs_to_m"]',
            '[data-testid^="plot"]',
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if await locator.count():
                    text = (await locator.inner_text(timeout=2000)).strip()
                    if text:
                        return self.clean_plot_text(text)
            except Exception:
                continue
        return ""

    def extract_imdb_plot(self, content: str) -> str:
        patterns = [
            r'data-testid=["\']plot-[^"\']+["\'][^>]*>(.*?)</span>\s*</span>\s*</span>',
            r'"plotText"\s*:\s*\{\s*"plainText"\s*:\s*"([^"]+)"',
            r'"description"\s*:\s*"([^"]+)"',
        ]
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
            if match:
                return self.clean_plot_text(match.group(1))
        return ""

    def clean_plot_text(self, text: str) -> str:
        text = re.sub(r"<[^>]+>", " ", text)
        text = text.replace("\\u0027", "'").replace('\\"', '"')
        return re.sub(r"\s+", " ", html.unescape(text)).strip()
