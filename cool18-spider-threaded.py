#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cool18 禁忌书屋「多线程优化版本」
使用线程池并发下载，简单易用
"""
import os
import re
import time
import random
import requests
import subprocess
import sys
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

if sys.platform == "win32":
    subprocess.run("chcp 65001", shell=True, capture_output=True)

BASE_URL = "https://www.cool18.com/bbs4/index.php?app=forum&act=threadview&tid="
INDEX_BASE = "https://www.cool18.com/bbs4/index.php?app=forum&act=gold&p={}"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
TIMEOUT = 15
RETRY = 3

# 配置常量
MAX_PAGES = 38
MIN_DELAY = 0.2  # 多线程版本的延迟
MAX_DELAY = 0.5
MAX_WORKERS = 8  # 线程池大小

OUTPUT_DIR = "output"
LIST_DIR = "list"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

MAIN_LIST = os.path.join(LIST_DIR, "main.list")
TEMP_LIST = os.path.join(LIST_DIR, "temp.list")

# 线程锁
file_lock = Lock()
print_lock = Lock()


# ---------- 优化的网络请求 ----------
# 创建一个全局session，复用连接
session = requests.Session()
session.headers.update(HEADERS)
# 设置连接池参数
adapter = requests.adapters.HTTPAdapter(
    pool_connections=20,
    pool_maxsize=20,
    max_retries=RETRY
)
session.mount('http://', adapter)
session.mount('https://', adapter)


def get_html(url):
    """线程安全的HTML获取函数"""
    for attempt in range(RETRY):
        try:
            r = session.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except requests.exceptions.RequestException as e:
            with print_lock:
                print(f"[warn] get {url} error: {e}  retry {attempt + 1}/{RETRY}...")
            if attempt < RETRY - 1:
                time.sleep(1)
        except Exception as e:
            with print_lock:
                print(f"[error] Unexpected error: {e}")
            break
    return None


# ---------- 工具函数 ----------
def safe_filename(name):
    name = re.sub(r'<[^>]+>', '', name)
    name = re.sub(r'[\\/:*?"<>|\s]', '_', name)
    return name.strip('. ') or 'untitled'


def read_list(path):
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


def write_list(path, lst):
    with open(path, 'w', encoding='utf-8') as f:
        for s in lst:
            f.write(s + '\n')


def extract_title(html):
    raw = re.search(r'<title>(.*?)</title>', html, flags=re.I | re.S)
    if not raw:
        return ""
    return re.split(r'[（(]', raw.group(1).strip(), maxsplit=1)[0].strip()[:30]


def extract_text(html):
    txt = []
    for m in re.findall(r'<div[^>]*class=["\']quote["\'][^>]*>(.*?)</div>', html, flags=re.I | re.S):
        m = re.sub(r'<br\s*/?>', '\n', m, flags=re.I)
        txt.append(re.sub(r'<[^>]+>', '', m))
    if not txt:
        divs = re.findall(r'<div[^>]*>(.*?)</div>', html, flags=re.I | re.S)
        if divs:
            longest = max(divs, key=len)
            longest = re.sub(r'<br\s*/?>', '\n', longest, flags=re.I)
            txt.append(re.sub(r'<[^>]+>', '', longest))
    return "\n".join(txt).strip()


def list_novels_one_page(html):
    novels = []
    for url, tid, title in re.findall(r'<a\s+href=["\']([^"\']*tid=(\d+)[^"\']*)["\'][^>]*>(.*?)</a>', html, flags=re.I):
        if "act=thread" in url or "act=threadview" in url:
            title = safe_filename(title.strip())
            novels.append({"title": title, "url": urljoin(BASE_URL, url), "tid": int(tid)})
    seen = set(); res = []
    for n in novels:
        if n["tid"] not in seen:
            seen.add(n["tid"])
            res.append(n)
    return res


def clean_final(text: str, inner_titles: list) -> str:
    # 1. 连续两个半角空格 -> 硬回车+两个半角空格
    text = re.sub(r'  +', '\n  ', text)
    # 2. 连续两个全角空格 -> 硬回车+两个全角空格
    text = re.sub(r'　　+', '\n　　', text)
    # 3. 删除内链标题
    for t in inner_titles:
        text = text.replace(t, '')
    # 4. 删除所有半角/全角空格
    text = re.sub(r'[ \u00A0\u3000]+', '', text)
    # 5. 删除纯空段
    lines = [ln for ln in text.splitlines() if ln.strip()]
    # 6. 每段前加两个全角空格
    lines = ['　　' + ln for ln in lines]
    return '\n'.join(lines)


# ---------- 多线程抓取函数 ----------
def fetch_page(url):
    """获取单个页面的内容"""
    html = get_html(url)
    if html:
        return url, html, extract_text(html)
    return url, None, None


def crawl_one_threaded(info):
    """多线程优化的单本小说抓取"""
    title, first_url, start_tid = info["title"], info["url"], info["tid"]
    with print_lock:
        print(f"【start】{title}  （首tid={start_tid}）")
    
    html = get_html(first_url)
    if not html:
        with print_lock:
            print(f"[fail] 首页下载失败 {first_url}")
        return
    
    base_prefix = extract_title(html)
    full_text = [extract_text(html)]
    inner_titles = []

    # ---------- 内链处理（并行） ----------
    inner_links = []
    for url, txt in re.findall(r'<a\s+href=["\']([^"\']*tid=\d+[^"\']*)["\'][^>]*>([^<]*\d+[^<]*)</a>', html, flags=re.I):
        full = urljoin(first_url, url)
        txt = re.sub(r'<[^>]+>', '', txt).strip()
        if re.search(r'\d+', txt):
            inner_links.append((full, txt))
            inner_titles.append(txt)

    if inner_links:
        with print_lock:
            print(f"    发现 {len(inner_links)} 个内链章节，并行抓取...")
        
        # 使用线程池并行抓取内链
        urls = [url for url, _ in reversed(inner_links)]
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(urls))) as executor:
            future_to_url = {executor.submit(fetch_page, url): url for url in urls}
            for future in as_completed(future_to_url):
                url, html_content, text_content = future.result()
                if text_content:
                    full_text.append(text_content)
        
        with print_lock:
            print(f"    内链并行抓取完成")

    # ---------- tid 递增（批量并行） ----------
    else:
        with print_lock:
            print("    无内链，启用并行 tid 递增。")
        
        tid = start_tid + 1
        fail_streak = 0
        batch_size = MAX_WORKERS  # 每批数量等于线程数
        
        while fail_streak < 3:
            # 准备一批URL
            batch_urls = [f"{BASE_URL}{tid + i}" for i in range(batch_size)]
            
            # 并行抓取
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_url = {executor.submit(fetch_page, url): url for url in batch_urls}
                results = []
                for future in as_completed(future_to_url):
                    url, html_content, text_content = future.result()
                    if html_content:
                        current_tid = int(url.split('=')[-1])
                        curr_prefix = extract_title(html_content)
                        results.append((current_tid, curr_prefix, text_content))
            
            # 处理结果
            results.sort()  # 按tid排序
            success_count = 0
            
            for current_tid, curr_prefix, text_content in results:
                if curr_prefix == base_prefix:
                    if fail_streak > 0:
                        with print_lock:
                            print(f"    tid={current_tid}  前缀恢复一致")
                    fail_streak = 0
                    if text_content:
                        full_text.append(text_content)
                        success_count += 1
                else:
                    fail_streak += 1
            
            if success_count > 0:
                with print_lock:
                    print(f"    并行抓取成功 {success_count} 个页面")
            else:
                with print_lock:
                    print(f"    并行抓取失败，fail_streak={fail_streak}")
            
            tid += batch_size
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    # ================= 保存文件 =================
    merged = clean_final('\n'.join(full_text), inner_titles)
    fname = os.path.join(OUTPUT_DIR, title + ".txt")
    
    with file_lock:
        with open(fname, "w", encoding="utf-8") as f:
            f.write(merged)
        with print_lock:
            print(f"【saved】{fname}\n")


# ---------- 主函数 ----------
def update_novels_threaded():
    """多线程更新小说"""
    print("\n====== 多线程更新小说 ======")
    main_list = read_list(MAIN_LIST)
    temp_list = read_list(TEMP_LIST)
    
    p = 1
    while p <= MAX_PAGES:
        index_url = INDEX_BASE.format(p)
        html = get_html(index_url)
        if not html:
            print(f"第{p}页下载失败，跳过")
            p += 1
            continue
            
        novels = list_novels_one_page(html)
        if not novels:
            print(f"第{p}页无新书，结束翻页")
            break
            
        print(f"\n------ 第{p}页 共{len(novels)} 本 ------")
        
        # 筛选需要下载的小说
        to_download = []
        for info in novels:
            title = info["title"]
            if os.path.exists(os.path.join(OUTPUT_DIR, title + ".txt")):
                continue
            to_download.append(info)
        
        if to_download:
            print(f"    需要下载 {len(to_download)} 本新书，开始多线程下载...")
            
            # 使用线程池并行下载多本小说
            with ThreadPoolExecutor(max_workers=min(MAX_WORKERS//2, len(to_download))) as executor:
                future_to_info = {executor.submit(crawl_one_threaded, info): info for info in to_download}
                for future in as_completed(future_to_info):
                    try:
                        future.result()
                    except Exception as e:
                        info = future_to_info[future]
                        print(f"[error] 下载 {info['title']} 失败: {e}")
            
            # 批量更新临时列表
            with file_lock:
                with open(TEMP_LIST, 'a', encoding='utf-8') as f:
                    for info in to_download:
                        f.write(info["title"] + '\n')
        
        print(f"------ 第{p}页处理完成 ------")
        p += 1
    
    merge_lists()
    print("多线程更新完成！")


def merge_lists():
    main = read_list(MAIN_LIST)
    temp = read_list(TEMP_LIST)
    if temp:
        write_list(MAIN_LIST, temp + main)
        if os.path.exists(TEMP_LIST):
            os.remove(TEMP_LIST)
        print("已合并列表，临时列表已删除。")


# ---------- 菜单 ----------
def menu():
    while True:
        print("\n=========  禁忌书屋抓取器（多线程版）  =========")
        print("1. 多线程更新小说")
        print("2. 调整线程数")
        print("0. 退出")
        choice = input("请选择：").strip()
        if choice == "1":
            update_novels_threaded()
        elif choice == "2":
            adjust_threads()
        elif choice == "0":
            print("再见！")
            break
        else:
            print("输入有误，请重选")


def adjust_threads():
    """调整线程数"""
    global MAX_WORKERS, MIN_DELAY, MAX_DELAY
    print(f"\n当前配置：")
    print(f"最大线程数: {MAX_WORKERS}")
    print(f"延迟范围: {MIN_DELAY}-{MAX_DELAY}秒")
    
    try:
        new_workers = input(f"输入新的线程数（当前{MAX_WORKERS}，回车跳过）：").strip()
        if new_workers:
            MAX_WORKERS = int(new_workers)
            
        new_min_delay = input(f"输入最小延迟（当前{MIN_DELAY}，回车跳过）：").strip()
        if new_min_delay:
            MIN_DELAY = float(new_min_delay)
            
        new_max_delay = input(f"输入最大延迟（当前{MAX_DELAY}，回车跳过）：").strip()
        if new_max_delay:
            MAX_DELAY = float(new_max_delay)
            
        print("参数更新成功！")
    except ValueError:
        print("输入无效，保持原设置")


if __name__ == "__main__":
    menu()
