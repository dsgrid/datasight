/** Markdown rendering with syntax highlighting and sanitization. */

import { Marked } from "marked";
import { markedHighlight } from "marked-highlight";
import hljs from "highlight.js/lib/core";
import sql from "highlight.js/lib/languages/sql";
import python from "highlight.js/lib/languages/python";
import json from "highlight.js/lib/languages/json";
import yaml from "highlight.js/lib/languages/yaml";
import DOMPurify from "dompurify";

// Register languages
hljs.registerLanguage("sql", sql);
hljs.registerLanguage("python", python);
hljs.registerLanguage("json", json);
hljs.registerLanguage("yaml", yaml);

const marked = new Marked(
  markedHighlight({
    highlight(code: string, lang: string) {
      if (lang && hljs.getLanguage(lang)) {
        return hljs.highlight(code, { language: lang }).value;
      }
      return hljs.highlightAuto(code).value;
    },
  }),
);

marked.setOptions({
  breaks: true,
  gfm: true,
});

/**
 * Render markdown to sanitized HTML.
 */
export function renderMarkdown(text: string): string {
  const raw = marked.parse(text) as string;
  return DOMPurify.sanitize(raw);
}

/**
 * Sanitize arbitrary HTML.
 */
export function sanitizeHtml(html: string): string {
  return DOMPurify.sanitize(html);
}
