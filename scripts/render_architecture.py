"""Render the architecture Markdown using its six maintained offline SVG figures.

No network or third-party packages. Narrative content is sourced from Markdown;
SVG diagrams and reader styles/controls are maintained in the checked-in HTML.
Run with --check to detect a narrative change that has not been rendered yet.
"""
import argparse
import html
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'docs' / 'architecture-and-implementation.md'
TARGET = SOURCE.with_suffix('.html')


def inline(text):
    tokens = re.split(r'(`[^`]+`|\[[^\]]+\]\([^)]+\)|\*\*.+?\*\*)', text)
    output = []
    for token in tokens:
        if token.startswith('`') and token.endswith('`'):
            output.append('<code>' + html.escape(token[1:-1]) + '</code>')
        elif token.startswith('**') and token.endswith('**'):
            output.append('<strong>' + inline(token[2:-2]) + '</strong>')
        elif (match := re.fullmatch(r'\[([^\]]+)\]\(([^)]+)\)', token)):
            output.append('<a href="' + html.escape(match[2], quote=True) + '">' + inline(match[1]) + '</a>')
        else:
            output.append(html.escape(token))
    return ''.join(output)


def render_blocks(markdown, figures):
    lines = markdown.splitlines()
    output, i = [], 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        if line.startswith('```'):
            language, content = line[3:], []
            i += 1
            while i < len(lines) and not lines[i].startswith('```'):
                content.append(lines[i])
                i += 1
            if language == 'mermaid':
                key = re.search(r'%% diagram: (\w+)', '\n'.join(content)).group(1)
                output.append(figures[key])
            else:
                output.append('<pre class="code-block"><code>' + html.escape('\n'.join(content)) + '</code></pre>')
            i += 1
            continue
        if re.fullmatch(r'<a id="s\d+"></a>', line):
            output.append(line)
            i += 1
            continue
        if match := re.match(r'^(#{1,3}) (.+)$', line):
            level = len(match[1])
            output.append(f'<h{level}>' + inline(match[2]) + f'</h{level}>')
            i += 1
            continue
        if line.startswith('|') and i + 1 < len(lines) and re.fullmatch(r'[| :\-]+', lines[i + 1]):
            cells = lambda row: [cell.strip() for cell in row.strip().strip('|').split('|')]
            output.append('<div class="table-scroll"><table><thead><tr>' + ''.join('<th scope="col">' + inline(cell) + '</th>' for cell in cells(line)) + '</tr></thead><tbody>')
            i += 2
            while i < len(lines) and lines[i].startswith('|'):
                output.append('<tr>' + ''.join('<td>' + inline(cell) + '</td>' for cell in cells(lines[i])) + '</tr>')
                i += 1
            output.append('</tbody></table></div>')
            continue
        if match := re.match(r'^(- |\d+\. )(.+)', line):
            tag = 'ul' if match[1] == '- ' else 'ol'
            pattern = r'^- (.+)' if tag == 'ul' else r'^\d+\. (.+)'
            output.append('<' + tag + '>')
            while i < len(lines) and (item := re.match(pattern, lines[i])):
                output.append('<li>' + inline(item[1]) + '</li>')
                i += 1
            output.append('</' + tag + '>')
            continue
        paragraph = [line]
        i += 1
        while i < len(lines) and lines[i].strip() and not re.match(r'^(#|```|<a id=|\||- |\d+\. )', lines[i]):
            paragraph.append(lines[i])
            i += 1
        output.append('<p>' + inline(' '.join(paragraph)) + '</p>')
    return '\n'.join(output)


def render(markdown, previous):
    figures = dict(re.findall(r'(<figure class="diagram" data-diagram="([^"]+)">[\s\S]*?</figure>)', previous))
    figures = {key: value for value, key in figures.items()}
    assert len(figures) == 6, 'Keep exactly six offline SVG diagrams'
    style = re.search(r'<style>([\s\S]*?)</style>', previous).group(1)
    script = re.search(r'<script>([\s\S]*?)</script>', previous).group(1)
    sections = re.findall(r'<a id="(s\d+)"></a>\s*## ([^\n]+)', markdown)
    assert len(sections) == 14, 'Keep exactly fourteen numbered sections'
    nav = ''.join('<a href="#' + key + '">' + html.escape(title) + '</a>' for key, title in sections)
    body = render_blocks(markdown, figures)
    return ('<!doctype html>\n<html lang="zh-CN"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta name="description" content="TrainPlane整体架构、功能实现、验证证据与优化空间；离线自包含文档。">'
            '<title>TrainPlane · 架构与实现说明</title><style>' + style + '</style></head>\n'
            '<body><div class="layout"><aside class="sidebar"><div class="brand">TrainPlane</div>'
            '<div class="subbrand">CPU YOLO · 架构与实现说明</div><nav aria-label="文档目录">' + nav + '</nav>'
            '<button type="button" class="print" id="print-document">打印 / 另存为PDF</button>'
            '<div class="offline">离线自包含 · 6张内嵌SVG<br>14节实现说明与证据。<br>不加载CDN或远程资源。</div>'
            '</aside><main class="paper">' + body + '<div class="footer">以当前代码与已有验收证据为准；新增能力与历史证据的边界见第13节。</div>'
            '</main></div><script>' + script + '</script></body></html>\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    current = TARGET.read_text(encoding='utf-8')
    expected = render(SOURCE.read_text(encoding='utf-8'), current)
    if args.check:
        if current != expected:
            raise SystemExit('HTML narrative is stale: run python scripts/render_architecture.py')
        print('PASS: Markdown and offline HTML narrative agree; six diagrams, fourteen sections')
    else:
        TARGET.write_text(expected, encoding='utf-8')
        print('Rendered:', TARGET)
