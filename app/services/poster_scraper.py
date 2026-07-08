from __future__ import annotations

import html
import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests


class PosterScraper:
    """Poster scraper using Playwright/Chromium."""

    def __init__(self, image_dir: str | Path):
        self.image_dir = Path(image_dir)

    async def download_poster(self, tconst: str) -> Path | None:
        self.image_dir.mkdir(parents=True, exist_ok=True)
        existing = self.existing_path(tconst)
        if existing:
            return existing

        from playwright.async_api import async_playwright

        browser = None
        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                page = await browser.new_page(
                    viewport={"width": 1366, "height": 1600},
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                )

                await page.goto(f"https://www.imdb.com/title/{tconst}/", wait_until="domcontentloaded", timeout=45000)
                await page.wait_for_timeout(2500)
                await self.dismiss_privacy_prompt(page)
                candidates = []
                content = await page.content()
                candidates.extend(self.extract_image_urls(content))
                candidates.extend(await self.extract_rendered_image_urls(page))

                media_link = await self.rendered_media_viewer_url(page, tconst) or self.extract_media_viewer_url(
                    content, tconst
                )
                if media_link:
                    await page.goto(media_link, wait_until="domcontentloaded", timeout=45000)
                    await page.wait_for_timeout(2500)
                    await self.dismiss_privacy_prompt(page)
                    media_content = await page.content()
                    candidates.extend(self.extract_image_urls(media_content))
                    candidates.extend(await self.extract_rendered_image_urls(page))

                poster_url = self.best_image_url(candidates)
                if not poster_url:
                    return None

                return self.save_image(tconst, poster_url)
        finally:
            if browser:
                await browser.close()

    async def dismiss_privacy_prompt(self, page) -> None:
        for selector in (
            "button:has-text('Accept')",
            "button:has-text('Accept all')",
            "button:has-text('I agree')",
            "button:has-text('Reject')",
        ):
            try:
                button = page.locator(selector).first
                if await button.count():
                    await button.click(timeout=1500)
                    await page.wait_for_timeout(500)
                    return
            except Exception:
                continue

    def existing_path(self, tconst: str) -> Path | None:
        for suffix in (".jpg", ".jpeg", ".png", ".webp"):
            path = self.image_dir / f"{tconst}{suffix}"
            if path.exists() and path.stat().st_size > 0:
                return path
        return None

    async def rendered_media_viewer_url(self, page, tconst: str) -> str | None:
        selectors = [
            f'a[href*="/title/{tconst}/mediaviewer/"][aria-label*="Poster"]',
            f'a[href*="/title/{tconst}/mediaviewer/"]',
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if await locator.count():
                    href = await locator.get_attribute("href")
                    if href:
                        return urljoin("https://www.imdb.com", href)
            except Exception:
                continue
        return None

    def extract_media_viewer_url(self, content: str, tconst: str) -> str | None:
        patterns = [
            r'href=["\'](/title/%s/mediaviewer/[^"\']+)["\']' % re.escape(tconst),
            r'href=\\?"(/title/%s/mediaviewer/[^"\\]+)\\?"' % re.escape(tconst),
        ]
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                return urljoin("https://www.imdb.com", html.unescape(match.group(1)))
        return None

    async def extract_rendered_image_urls(self, page) -> list[str]:
        try:
            rows = await page.evaluate(
                """
                () => Array.from(document.images).map((img) => ({
                    src: img.currentSrc || img.src || "",
                    srcset: img.getAttribute("srcset") || ""
                }))
                """
            )
        except Exception:
            return []

        candidates = []
        for row in rows:
            src = row.get("src") or ""
            if self.looks_like_imdb_image(src):
                candidates.append(src)
            candidates.extend(self.urls_from_srcset(row.get("srcset") or ""))
        return candidates

    def extract_image_urls(self, content: str) -> list[str]:
        candidates: list[str] = []
        meta_patterns = [
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
        ]
        for pattern in meta_patterns:
            candidates.extend(re.findall(pattern, content, re.IGNORECASE))

        for script_text in re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            content,
            re.IGNORECASE | re.DOTALL,
        ):
            try:
                data = json.loads(html.unescape(script_text).strip())
            except Exception:
                continue
            if isinstance(data, dict):
                image = data.get("image")
                if isinstance(image, str):
                    candidates.append(image)
                elif isinstance(image, list):
                    candidates.extend(str(item) for item in image)

        candidates.extend(self.urls_from_srcset_text(content))
        candidates.extend(
            re.findall(
                r'https?:\\?/\\?/m\.media-amazon\.com/images/M/[^"\'<>\s\\]+',
                content,
                re.IGNORECASE,
            )
        )
        return [self.clean_image_url(candidate) for candidate in candidates if candidate]

    def urls_from_srcset_text(self, content: str) -> list[str]:
        urls = []
        for srcset in re.findall(r'srcset=["\']([^"\']+)["\']', content, re.IGNORECASE):
            urls.extend(self.urls_from_srcset(html.unescape(srcset)))
        return urls

    def urls_from_srcset(self, srcset: str) -> list[str]:
        urls = []
        for part in srcset.split(","):
            url = part.strip().split(" ")[0]
            if self.looks_like_imdb_image(url):
                urls.append(url)
        return urls

    def looks_like_imdb_image(self, url: str) -> bool:
        return "m.media-amazon.com/images/M/" in self.clean_image_url(url)

    def clean_image_url(self, url: str) -> str:
        url = html.unescape(url).strip().strip('"').strip("'")
        url = url.replace("\\u002F", "/").replace("\\/", "/")
        if url.startswith("//"):
            url = f"https:{url}"
        return url

    def best_image_url(self, candidates: list[str]) -> str | None:
        cleaned = []
        seen = set()
        for candidate in candidates:
            url = self.clean_image_url(candidate)
            if not self.looks_like_imdb_image(url) or url in seen:
                continue
            seen.add(url)
            cleaned.append(url)
        if not cleaned:
            return None

        def score(url: str) -> tuple[int, int, int]:
            size_match = re.search(r'[._](?:UX|UY|SX|SY|CR\d+,\d+,)(\d+)', url)
            explicit_size = int(size_match.group(1)) if size_match else 0
            has_v1_variant = 1 if "_V1_" in url else 0
            has_extension = 1 if re.search(r'\.(?:jpg|jpeg|png|webp)(?:[._?]|$)', url, re.IGNORECASE) else 0
            return explicit_size, has_v1_variant, has_extension

        return sorted(cleaned, key=score, reverse=True)[0]

    def save_image(self, tconst: str, url: str) -> Path | None:
        url = self.clean_image_url(url)
        response = requests.get(
            url,
            timeout=45,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Referer": f"https://www.imdb.com/title/{tconst}/",
            },
        )
        response.raise_for_status()
        content_type = response.headers.get("content-type", "").lower()
        suffix = ".jpg"
        if "png" in content_type:
            suffix = ".png"
        elif "webp" in content_type:
            suffix = ".webp"

        path = self.image_dir / f"{tconst}{suffix}"
        path.write_bytes(response.content)
        return path if path.exists() and path.stat().st_size > 0 else None
