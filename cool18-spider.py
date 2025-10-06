#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cool18 禁忌书屋「菜单+列表管理+详细输出」完整抓取
python cool18_spider.py
"""
import os
import re
import time
import random
import requests
import subprocess
import sys
from urllib.parse import urljoin

if sys.platform == "win32":
    subprocess.run("chcp 65001", shell=True, capture_output=True)

BASE_URL = "https://www.cool18.com/bbs4/index.php?app=forum&act=threadview&tid="
INDEX_BASE = "https://www.cool18.com/bbs4/index.php?app=forum&act=gold&p={}"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
TIMEOUT = 15
RETRY = 3
OUTPUT_DIR = "output"
LIST_DIR = "list"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

MAIN_LIST = os.path.join(LIST_DIR, "main.list")
TEMP_LIST = os.path.join(LIST_DIR, "temp.list")


# ---------- 通用 ----------
def get_html(url):
    for _ in range(RETRY):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:
            print(f"[warn] get {url} error: {e}  retry...")
            time.sleep(2)
    return None


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


# ================= 最终清洗 =================
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
# ==========================================


def crawl_one(info):
    title, first_url, start_tid = info["title"], info["url"], info["tid"]
    print(f"【start】{title}  （首tid={start_tid}）")
    html = get_html(first_url)
    if not html:
        print(f"[fail] 首页下载失败 {first_url}")
        return
    base_prefix = extract_title(html)
    full_text = [extract_text(html)]

    # 内链标题变量
    inner_titles = []

    # ---------- 内链处理 ----------
    inner_links = []
    for url, txt in re.findall(r'<a\s+href=["\']([^"\']*tid=\d+[^"\']*)["\'][^>]*>([^<]*\d+[^<]*)</a>', html, flags=re.I):
        full = urljoin(first_url, url)
        txt = re.sub(r'<[^>]+>', '', txt).strip()
        if re.search(r'\d+', txt):
            inner_links.append((full, txt))
            inner_titles.append(txt)  # 保存标题

    if inner_links:
        print(f"    发现 {len(inner_links)} 个内链章节（按源码倒序抓，不保留链接文字）：")
        for url, txt in reversed(inner_links):
            h = get_html(url)
            if h:
                full_text.append(extract_text(h))  # 不插入 txt
                print(f"    倒序抓取内链【{txt}】已抓取。")

    # ---------- tid 递增 ----------
    else:
        print("    无内链，启用 tid 递增。")
        tid = start_tid + 1
        fail_streak = 0
        while True:
            url = f"{BASE_URL}{tid}"
            h = get_html(url)
            if h is None:
                print(f"    tid={tid}  404/跳转，fail_streak={fail_streak+1}")
                fail_streak += 1
                if fail_streak >= 3:
                    print("    连续3次失败/前缀变化，结束本书抓取。")
                    break
                tid += 1
                continue
            curr_prefix = extract_title(h)
            if curr_prefix == base_prefix:
                if fail_streak > 0:
                    print(f"    tid={tid}  前缀恢复一致，计数器清零。")
                fail_streak = 0
                txt = extract_text(h)
                if txt:
                    full_text.append(txt)
                print(f"    tid={tid}  已抓取。")
            else:
                fail_streak += 1
                print(f"    tid={tid}  前缀变化（{curr_prefix}≠{base_prefix}），放弃本页  fail_streak={fail_streak}")
                if fail_streak >= 3:
                    print("    连续3次前缀变化，结束本书抓取。")
                    break
            tid += 1
            time.sleep(random.uniform(0.5, 1.5))

    # ================= 合并后、保存前：统一清洗 =================
    merged = clean_final('\n'.join(full_text), inner_titles)

    fname = os.path.join(OUTPUT_DIR, title + ".txt")
    with open(fname, "w", encoding="utf-8") as f:
        f.write(merged)
    print(f"【saved】{fname}\n")


# ---------- 列表管理（带详细输出） ----------
def update_novels():
    """主流程：更新小说（带详细输出）"""
    print("\n====== 更新小说 ======")
    main_list = read_list(MAIN_LIST)
    temp_list = read_list(TEMP_LIST)
    if not main_list and not temp_list:
        print("首次运行，建立正式列表...")
    p = 1
    while p <= 38:
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
        for info in novels:
            title = info["title"]
            if os.path.exists(os.path.join(OUTPUT_DIR, title + ".txt")):
                if title in main_list:
                    merge_lists()
                    continue
                if read_list(TEMP_LIST) and read_list(TEMP_LIST)[-1] != title:
                    with open(TEMP_LIST, 'a', encoding='utf-8') as f:
                        f.write(title + '\n')
                    print(f"    [列表] {title} 已存在，追加到临时列表尾部")
                    continue
                continue
            print(f"    [下载] {title}")
            crawl_one(info)
            with open(TEMP_LIST, 'a', encoding='utf-8') as f:
                f.write(title + '\n')
            print(f"    [列表] {title} 已记入临时列表")
            time.sleep(random.uniform(1, 2))
        print(f"------ 第{p}页处理完成 ------")
        p += 1
    merge_lists()
    print("更新完成！")


def download_old_novels():
    """下载旧小说：只抓正式列表中未下载的"""
    print("\n====== 下载旧小说 ======")
    main_list = read_list(MAIN_LIST)
    if not main_list:
        print("正式列表为空，请先「更新小说」")
        return
    for title in main_list:
        if os.path.exists(os.path.join(OUTPUT_DIR, title + ".txt")):
            print(f"    [跳过] {title} 已存在")
            continue
        tid = int(re.search(r'\d+', title).group()) if re.search(r'\d+', title) else 0
        if not tid:
            continue
        info = {"title": title, "url": f"{BASE_URL}{tid}", "tid": tid}
        print(f"    [下载] {title}")
        crawl_one(info)
        time.sleep(random.uniform(1, 2))
    print("旧小说下载完成！")


def merge_lists():
    main = read_list(MAIN_LIST)
    temp = read_list(TEMP_LIST)
    if temp:
        write_list(MAIN_LIST, temp + main)
        os.remove(TEMP_LIST)
        print("已合并列表，临时列表已删除。")


# ---------- 菜单 ----------
def menu():
    while True:
        print("\n=========  禁忌书屋抓取器  =========")
        print("1. 更新小说（带列表管理）")
        print("2. 下载旧小说（补全未下载）")
        print("0. 退出")
        choice = input("请选择：").strip()
        if choice == "1":
            update_novels()
        elif choice == "2":
            download_old_novels()
        elif choice == "0":
            print("再见！")
            break
        else:
            print("输入有误，请重选")


if __name__ == "__main__":
    menu()
