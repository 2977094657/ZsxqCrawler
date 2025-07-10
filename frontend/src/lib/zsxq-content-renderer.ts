/**
 * 知识星球内容渲染工具
 * 处理知识星球特殊标签格式的解析和渲染
 */

/**
 * 解码URL编码的字符串
 */
function decodeTitle(encodedTitle: string): string {
  try {
    return decodeURIComponent(encodedTitle);
  } catch (error) {
    console.warn('解码失败:', encodedTitle, error);
    return encodedTitle;
  }
}

/**
 * 渲染知识星球内容中的特殊标签
 */
export function renderZsxqContent(content: string): string {
  if (!content) return '';

  let renderedContent = content;

  // 处理粗体文本标签 <e type="text_bold" title="..." />
  renderedContent = renderedContent.replace(
    /<e\s+type="text_bold"\s+title="([^"]+)"\s*\/>/g,
    (match, encodedTitle) => {
      const decodedTitle = decodeTitle(encodedTitle);
      return `<strong>${decodedTitle}</strong>`;
    }
  );

  // 处理话题标签 <e type="hashtag" hid="..." title="..." />
  renderedContent = renderedContent.replace(
    /<e\s+type="hashtag"\s+hid="([^"]+)"\s+title="([^"]+)"\s*\/>/g,
    (match, hid, encodedTitle) => {
      const decodedTitle = decodeTitle(encodedTitle);
      // 移除标题中的 # 符号，因为我们会在显示时添加
      const cleanTitle = decodedTitle.replace(/^#|#$/g, '');
      const encodedCleanTitle = encodeURIComponent(cleanTitle);
      
      return `<br><a href="https://wx.zsxq.com/tags/${encodedCleanTitle}/${hid}" target="_blank" rel="noopener noreferrer" class="text-blue-600 hover:text-blue-800 font-medium no-underline transition-colors">#${cleanTitle}</a>`;
    }
  );

  // 处理斜体文本标签 <e type="text_italic" title="..." />
  renderedContent = renderedContent.replace(
    /<e\s+type="text_italic"\s+title="([^"]+)"\s*\/>/g,
    (match, encodedTitle) => {
      const decodedTitle = decodeTitle(encodedTitle);
      return `<em>${decodedTitle}</em>`;
    }
  );

  // 处理删除线文本标签 <e type="text_strikethrough" title="..." />
  renderedContent = renderedContent.replace(
    /<e\s+type="text_strikethrough"\s+title="([^"]+)"\s*\/>/g,
    (match, encodedTitle) => {
      const decodedTitle = decodeTitle(encodedTitle);
      return `<del>${decodedTitle}</del>`;
    }
  );

  // 处理下划线文本标签 <e type="text_underline" title="..." />
  renderedContent = renderedContent.replace(
    /<e\s+type="text_underline"\s+title="([^"]+)"\s*\/>/g,
    (match, encodedTitle) => {
      const decodedTitle = decodeTitle(encodedTitle);
      return `<u>${decodedTitle}</u>`;
    }
  );

  // 处理链接标签 <e type="web_url" href="..." title="..." />
  renderedContent = renderedContent.replace(
    /<e\s+type="web_url"\s+href="([^"]+)"\s+title="([^"]+)"\s*\/>/g,
    (match, href, encodedTitle) => {
      const decodedTitle = decodeTitle(encodedTitle);
      const decodedHref = decodeTitle(href);

      // 判断是否为知识星球链接
      const isZsxqLink = decodedHref.includes('t.zsxq.com') || decodedHref.includes('zsxq.com');
      const linkIcon = isZsxqLink
        ? `<img src="https://zsxq.com/assets/img/zsxq_logo@2x.png" alt="知识星球" style="display: inline-block; width: 16px; height: 16px; margin-right: 4px; vertical-align: middle; filter: brightness(0) saturate(100%) invert(47%) sepia(69%) saturate(959%) hue-rotate(121deg) brightness(98%) contrast(86%);" />`
        : `<svg style="display: inline-block; width: 16px; height: 16px; margin-right: 4px; vertical-align: middle;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path></svg>`;

      return `<span style="display: inline-flex; align-items: center; vertical-align: middle;"><a href="${decodedHref}" target="_blank" rel="noopener noreferrer" style="display: inline-flex; align-items: center; text-decoration: none; color: #2563eb;">${linkIcon}<span style="vertical-align: middle;">${decodedTitle}</span></a></span>`;
    }
  );

  // 处理web链接标签 <e type="web" href="..." title="..." />
  renderedContent = renderedContent.replace(
    /<e\s+type="web"\s+href="([^"]+)"\s+title="([^"]+)"\s*\/>/g,
    (match, href, encodedTitle) => {
      const decodedTitle = decodeTitle(encodedTitle);
      const decodedHref = decodeTitle(href);

      // 判断是否为知识星球链接
      const isZsxqLink = decodedHref.includes('t.zsxq.com') || decodedHref.includes('zsxq.com');
      const linkIcon = isZsxqLink
        ? `<img src="https://zsxq.com/assets/img/zsxq_logo@2x.png" alt="知识星球" style="display: inline-block; width: 16px; height: 16px; margin-right: 4px; vertical-align: middle; filter: brightness(0) saturate(100%) invert(47%) sepia(69%) saturate(959%) hue-rotate(121deg) brightness(98%) contrast(86%);" />`
        : `<svg style="display: inline-block; width: 16px; height: 16px; margin-right: 4px; vertical-align: middle;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path></svg>`;

      return `<span style="display: inline-flex; align-items: center; vertical-align: middle;"><a href="${decodedHref}" target="_blank" rel="noopener noreferrer" style="display: inline-flex; align-items: center; text-decoration: none; color: #2563eb;">${linkIcon}<span style="vertical-align: middle;">${decodedTitle}</span></a></span>`;
    }
  );

  // 处理用户提及标签 <e type="mention" uid="..." title="..." />
  renderedContent = renderedContent.replace(
    /<e\s+type="mention"\s+uid="([^"]+)"\s+title="([^"]+)"\s*\/>/g,
    (match, uid, encodedTitle) => {
      const decodedTitle = decodeTitle(encodedTitle);
      // 移除标题中已有的@符号，避免重复显示
      const cleanTitle = decodedTitle.replace(/^@+/, '');

      return `<br><span class="text-green-600 font-medium">@${cleanTitle}</span>`;
    }
  );

  return renderedContent;
}

/**
 * 为React组件提供的安全HTML渲染
 */
export function createSafeHtml(content: string) {
  const renderedContent = renderZsxqContent(content);
  return { __html: renderedContent };
}

/**
 * 提取内容中的纯文本（移除所有HTML标签）
 */
export function extractPlainText(content: string): string {
  if (!content) return '';

  // 先渲染特殊标签
  const renderedContent = renderZsxqContent(content);

  // 移除所有HTML标签
  return renderedContent.replace(/<[^>]*>/g, '').trim();
}

/**
 * 在文本中高亮搜索关键词
 */
export function highlightSearchTerm(content: string, searchTerm: string): string {
  if (!content || !searchTerm) return content;

  // 转义搜索词中的特殊字符
  const escapedSearchTerm = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

  // 创建正则表达式，忽略大小写
  const regex = new RegExp(`(${escapedSearchTerm})`, 'gi');

  // 替换匹配的文本，添加黄色高亮样式
  return content.replace(regex, '<mark style="background-color: #fef08a; color: #000; padding: 0 2px; border-radius: 2px;">$1</mark>');
}

/**
 * 为React组件提供的安全HTML渲染（带搜索高亮）
 */
export function createSafeHtmlWithHighlight(content: string, searchTerm?: string) {
  let renderedContent = renderZsxqContent(content);

  // 如果有搜索词，添加高亮
  if (searchTerm) {
    renderedContent = highlightSearchTerm(renderedContent, searchTerm);
  }

  return { __html: renderedContent };
}


