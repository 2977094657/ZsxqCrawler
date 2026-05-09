/**
 * API客户端 - 与后端FastAPI服务通信
 */

const API_BASE_URL = (process.env.NEXT_PUBLIC_API_BASE_URL?.trim() || 'http://localhost:8208').replace(/\/$/, '');

// 类型定义
export interface ApiResponse<T = any> {
  data?: T;
  message?: string;
  error?: string;
}

export interface Task {
  task_id: string;
  type: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  message: string;
  result?: any;
  created_at: string;
  updated_at: string;
}

export interface TaskCreateResponse {
  task_id: string;
  message: string;
}

export interface SimpleResponse {
  success: boolean;
  message: string;
  [key: string]: unknown;
}

export interface DatabaseStats {
  configured?: boolean;
  has_local_data?: boolean;
  topic_database: {
    stats: Record<string, number>;
    timestamp_info: {
      total_topics: number;
      oldest_timestamp: string;
      newest_timestamp: string;
      has_data: boolean;
    };
  };
  file_database: {
    stats: Record<string, number>;
  };
}

export interface Topic {
  topic_id: string;
  title: string;
  create_time: string;
  likes_count: number;
  comments_count: number;
  reading_count: number;
  type: string;
  imported_at?: string;
}

export interface FileItem {
  file_id: number;
  name: string;
  size: number;
  download_count: number;
  create_time: string;
  download_status: string;
}

export interface FileStatus {
  file_id: number;
  name: string;
  size: number;
  download_status: string;
  local_exists: boolean;
  local_size: number;
  local_path?: string;
  is_complete: boolean;
}

export interface FileStats {
  database_stats: Record<string, number>;
  download_stats: {
    total_files: number;
    downloaded: number;
    pending: number;
    failed: number;
  };
}

export type RefreshTopicResponse =
  | {
      success: true;
      message: string;
      updated_data: {
        likes_count: number;
        comments_count: number;
        reading_count: number;
        readers_count: number;
      };
    }
  | {
      success: false;
      message: string;
    };

export interface PaginatedResponse<T> {
  data: T[];
  pagination: {
    page: number;
    per_page: number;
    total: number;
    pages: number;
  };
}

export interface Group {
  account?: Account;
  group_id: number;
  name: string;
  type: string;
  background_url?: string;
  description?: string;
  create_time?: string;
  subscription_time?: string;
  expiry_time?: string;
  join_time?: string;
  last_active_time?: string;
  status?: string;
  source?: string; // "account" | "local" | "account|local"
  size_bytes?: number; // 本地存储占用大小，单位：字节
  is_trial?: boolean;
  trial_end_time?: string;
  membership_end_time?: string;
  owner?: {
    user_id: number;
    name: string;
    alias?: string;
    avatar_url?: string;
    description?: string;
  };
  statistics?: {
    members?: {
      count: number;
    };
    topics?: {
      topics_count: number;
      answers_count: number;
      digests_count: number;
    };
    files?: {
      count: number;
    };
  };
}

export interface GroupStats {
  group_id: number;
  topics_count: number;
  users_count: number;
  latest_topic_time?: string;
  earliest_topic_time?: string;
  total_likes: number;
  total_comments: number;
  total_readings: number;
}

export interface GroupTag {
  tag_id: number;
  tag_name: string;
  hid?: string;
  topic_count: number;
  created_at?: string;
}

export interface ExportManifestGroup {
  group_id: string;
  name: string;
  type?: string;
  background_url?: string;
  cover_url?: string;
  cover_image_data_url?: string;
  owner?: {
    user_id?: number;
    name?: string;
    alias?: string;
    avatar_url?: string;
    description?: string;
  };
  statistics?: Record<string, any>;
  members_count?: number;
  topics_count?: number;
  files_count?: number;
  description?: string;
  create_time?: string;
  subscription_time?: string;
  join_time?: string;
  expiry_time?: string;
  last_active_time?: string;
  status?: string;
  is_trial?: boolean;
  trial_end_time?: string;
  membership_end_time?: string;
  directory?: string;
  size_bytes?: number;
}

export interface ExportManifest {
  manifest_version: number;
  app: string;
  export_type: 'single_group' | 'all_output';
  exported_at: string;
  source_root: string;
  data_size_bytes: number;
  groups_count: number;
  groups: ExportManifestGroup[];
}

export interface ImportConflict {
  group_id: string;
  paths: string[];
}

export interface ImportPreview {
  success: boolean;
  manifest: ExportManifest;
  groups: ExportManifestGroup[];
  conflicts: ImportConflict[];
  can_import: boolean;
}

export interface ImportConfirmResult {
  success: boolean;
  message: string;
  manifest: ExportManifest;
  groups: ExportManifestGroup[];
}

export interface Account {
  id: string;
  name?: string;
  cookie?: string; // 已掩码
  is_default?: boolean;
  created_at?: string;
}

export interface AccountSelf {
  account_id: string;
  uid?: string;
  name?: string;
  avatar_url?: string;
  location?: string;
  user_sid?: string;
  grade?: string;
  fetched_at?: string;
  raw_json?: any;
}

// API客户端类
class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  private async request<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`;
    
    const response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
    }

    return response.json();
  }

  // 健康检查
  async healthCheck() {
    return this.request('/api/health');
  }

  // 配置相关
  async getConfig() {
    return this.request('/api/config');
  }

  async updateConfig(config: { cookie: string }) {
    return this.request('/api/config', {
      method: 'POST',
      body: JSON.stringify(config),
    });
  }

  // 数据库统计
  async getDatabaseStats(): Promise<DatabaseStats> {
    return this.request('/api/database/stats');
  }

  // 任务相关
  async getTasks(): Promise<Task[]> {
    return this.request('/api/tasks');
  }

  async getTask(taskId: string): Promise<Task> {
    return this.request(`/api/tasks/${taskId}`);
  }

  async stopTask(taskId: string) {
    return this.request(`/api/tasks/${taskId}/stop`, {
      method: 'POST',
    });
  }

  // 爬取相关
  async crawlHistorical(groupId: number | string, pages: number = 10, perPage: number = 20, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }): Promise<TaskCreateResponse> {
    return this.request(`/api/crawl/historical/${groupId}`, {
      method: 'POST',
      body: JSON.stringify({
        pages,
        per_page: perPage,
        ...crawlSettings
      }),
    });
  }

  async crawlAll(groupId: number | string, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }): Promise<TaskCreateResponse> {
    return this.request(`/api/crawl/all/${groupId}`, {
      method: 'POST',
      body: JSON.stringify(crawlSettings || {}),
    });
  }

  async crawlIncremental(groupId: number | string, pages: number = 10, perPage: number = 20, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }): Promise<TaskCreateResponse> {
    return this.request(`/api/crawl/incremental/${groupId}`, {
      method: 'POST',
      body: JSON.stringify({
        pages,
        per_page: perPage,
        ...crawlSettings
      }),
    });
  }

  async crawlLatestUntilComplete(groupId: number | string, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }): Promise<TaskCreateResponse> {
    return this.request(`/api/crawl/latest-until-complete/${groupId}`, {
      method: 'POST',
      body: JSON.stringify(crawlSettings || {}),
    });
  }

  async getTopicDetail(topicId: number | string, groupId: number | string) {
    // 统一转为字符串，避免大整数在前端被 Number 处理后精度丢失
    const id = String(topicId);
    return this.request(`/api/topics/${id}/${groupId}`);
  }

  async refreshTopic(topicId: number | string, groupId: number | string): Promise<RefreshTopicResponse> {
    const id = String(topicId);
    return this.request(`/api/topics/${id}/${groupId}/refresh`, {
      method: 'POST',
    });
  }

  // 导出话题为 Markdown：默认 zip（含 assets/ 头像与图片，离线可读）；
  // 传 format='md' 可降级为单文件 .md。
  getTopicMarkdownExportUrl(
    topicId: number | string,
    groupId: number | string,
    fetchArticle: boolean = true,
    format: 'zip' | 'md' = 'zip',
  ): string {
    const id = String(topicId);
    const params = new URLSearchParams({
      fetch_article: fetchArticle ? 'true' : 'false',
      format,
    });
    return `${this.baseUrl}/api/topics/${id}/${groupId}/export-md?${params.toString()}`;
  }

  async deleteSingleTopic(groupId: number | string, topicId: number | string) {
    return this.request(`/api/topics/${topicId}/${groupId}`, {
      method: 'DELETE',
    });
  }

  // 单个话题采集（测试特殊话题）
  async fetchSingleTopic(groupId: number | string, topicId: number | string, fetchComments: boolean = false) {
    const id = String(topicId);
    const params = new URLSearchParams();
    if (fetchComments) params.append('fetch_comments', 'true');
    const url = `/api/topics/fetch-single/${groupId}/${id}${params.toString() ? '?' + params.toString() : ''}`;
    return this.request(url, { method: 'POST' });
  }

  // 获取代理图片URL，解决防盗链问题
  getProxyImageUrl(originalUrl: string, groupId?: string): string {
    if (!originalUrl) return '';
    const params = new URLSearchParams({ url: originalUrl });
    if (groupId) {
      params.append('group_id', groupId);
    }
    return `${API_BASE_URL}/api/proxy-image?${params.toString()}`;
  }

  // 获取本地缓存图片URL
  getLocalImageUrl(groupId: string, localPath: string): string {
    if (!localPath) return '';
    return `${API_BASE_URL}/api/groups/${groupId}/images/${encodeURIComponent(localPath)}`;
  }

  // 获取本地缓存视频URL
  getLocalVideoUrl(groupId: string, videoFilename: string): string {
    if (!videoFilename) return '';
    return `${API_BASE_URL}/api/groups/${groupId}/videos/${encodeURIComponent(videoFilename)}`;
  }

  // 图片缓存管理
  async getImageCacheInfo(groupId: string): Promise<Record<string, unknown>> {
    return this.request(`/api/cache/images/info/${groupId}`);
  }

  async clearImageCache(groupId: string): Promise<SimpleResponse> {
    return this.request(`/api/cache/images/${groupId}`, {
      method: 'DELETE',
    });
  }

  // 群组相关
  async getGroupInfo(groupId: number | string): Promise<Record<string, unknown>> {
    return this.request(`/api/groups/${groupId}/info`);
  }

  // 文件相关
  async collectFiles(groupId: number | string): Promise<TaskCreateResponse> {
    return this.request(`/api/files/collect/${groupId}`, {
      method: 'POST',
    });
  }

  async downloadFiles(groupId: number | string, maxFiles?: number, sortBy: string = 'download_count',
                     downloadInterval: number = 1.0, longSleepInterval: number = 60.0,
                     filesPerBatch: number = 10, downloadIntervalMin?: number,
                     downloadIntervalMax?: number, longSleepIntervalMin?: number,
                     longSleepIntervalMax?: number): Promise<TaskCreateResponse> {
    const requestBody: any = {
      max_files: maxFiles,
      sort_by: sortBy,
      download_interval: downloadInterval,
      long_sleep_interval: longSleepInterval,
      files_per_batch: filesPerBatch
    };

    // 如果提供了随机间隔范围参数，则添加到请求中
    if (downloadIntervalMin !== undefined) {
      requestBody.download_interval_min = downloadIntervalMin;
      requestBody.download_interval_max = downloadIntervalMax;
      requestBody.long_sleep_interval_min = longSleepIntervalMin;
      requestBody.long_sleep_interval_max = longSleepIntervalMax;
    }

    return this.request(`/api/files/download/${groupId}`, {
      method: 'POST',
      body: JSON.stringify(requestBody),
    });
  }

  async clearFileDatabase(groupId: number | string) {
    return this.request(`/api/files/clear/${groupId}`, {
      method: 'POST',
    });
  }

  async clearTopicDatabase(groupId: number | string) {
    return this.request(`/api/topics/clear/${groupId}`, {
      method: 'POST',
    });
  }

  async getFileStats(groupId: number | string): Promise<FileStats> {
    return this.request(`/api/files/stats/${groupId}`);
  }

  async downloadSingleFile(groupId: number | string, fileId: number, fileName?: string, fileSize?: number): Promise<TaskCreateResponse> {
    const params = new URLSearchParams();
    if (fileName) params.append('file_name', fileName);
    if (fileSize !== undefined) params.append('file_size', fileSize.toString());

    const url = `/api/files/download-single/${groupId}/${fileId}${params.toString() ? '?' + params.toString() : ''}`;
    return this.request(url, {
      method: 'POST',
    });
  }

  async getFileStatus(groupId: number | string, fileId: number): Promise<FileStatus> {
    return this.request(`/api/files/status/${groupId}/${fileId}`);
  }

  async checkLocalFileStatus(groupId: number | string, fileName: string, fileSize: number): Promise<Record<string, unknown>> {
    const params = new URLSearchParams({
      file_name: fileName,
      file_size: fileSize.toString(),
    });
    return this.request(`/api/files/check-local/${groupId}?${params}`);
  }

  // 数据查询
  async getTopics(page: number = 1, perPage: number = 20, search?: string): Promise<PaginatedResponse<Topic>> {
    const params = new URLSearchParams({
      page: page.toString(),
      per_page: perPage.toString(),
    });
    
    if (search) {
      params.append('search', search);
    }

    const response = await this.request<{topics: Topic[], pagination: any}>(`/api/topics?${params}`);
    return {
      data: response.topics,
      pagination: response.pagination,
    };
  }

  async getFiles(groupId: number | string, page: number = 1, perPage: number = 20, status?: string): Promise<PaginatedResponse<FileItem>> {
    const params = new URLSearchParams({
      page: page.toString(),
      per_page: perPage.toString(),
    });

    if (status) {
      params.append('status', status);
    }

    const response = await this.request<{files: FileItem[], pagination: any}>(`/api/files/${groupId}?${params}`);
    return {
      data: response.files,
      pagination: response.pagination,
    };
  }

  // 群组相关
  async refreshLocalGroups(): Promise<{ success: boolean; count: number; groups: number[]; error?: string }> {
    return this.request('/api/local-groups/refresh', {
      method: 'POST',
    });
  }

  async getGroups(): Promise<{groups: Group[], total: number}> {
    return this.request('/api/groups');
  }

  async getGroupTopics(groupId: number | string, page: number = 1, perPage: number = 20, search?: string): Promise<PaginatedResponse<Topic>> {
    // 🧪 调试输出：请求参数
    console.log('[apiClient.getGroupTopics] request params:', { groupId, page, perPage, search });

    const params = new URLSearchParams({
      page: page.toString(),
      per_page: perPage.toString(),
    });

    if (search) {
      params.append('search', search);
    }

    const url = `/api/groups/${groupId}/topics?${params}`;
    const response = await this.request<{topics: Topic[], pagination: any}>(url);

    // 🧪 调试输出：原始返回中的 topic_id（前 10 条）
    try {
      const debugTopics = (response.topics || []).slice(0, 10).map((t: any) => ({
        topic_id: t.topic_id,
        title: t.title,
      }));
      console.log('[apiClient.getGroupTopics] raw response topics (first 10):', debugTopics);
    } catch (e) {
      console.warn('[apiClient.getGroupTopics] debug log failed:', e);
    }

    const result: PaginatedResponse<Topic> = {
      data: response.topics,
      pagination: response.pagination,
    };

    // 🧪 调试输出：返回给调用方的数据结构
    try {
      const offerTopic = (result.data || []).find((t: any) =>
        typeof t.title === 'string' && t.title.startsWith('Offer选择')
      );
      if (offerTopic) {
        console.log('[apiClient.getGroupTopics] Offer topic in result:', {
          topic_id: (offerTopic as any).topic_id,
          title: offerTopic.title,
        });
      } else {
        console.log('[apiClient.getGroupTopics] Offer topic not found in result');
      }
    } catch (e) {
      console.warn('[apiClient.getGroupTopics] debug Offer topic failed:', e);
    }

    return result;
  }

  async getGroupStats(groupId: number | string): Promise<GroupStats> {
    return this.request(`/api/groups/${groupId}/stats`);
  }

  getGroupExportUrl(groupId: number | string): string {
    return `${this.baseUrl}/api/groups/${groupId}/export`;
  }

  getAllExportUrl(): string {
    return `${this.baseUrl}/api/export/all`;
  }

  async previewImportArchive(file: File): Promise<ImportPreview> {
    return this.uploadZip<ImportPreview>('/api/import/preview', file);
  }

  async confirmImportArchive(file: File): Promise<ImportConfirmResult> {
    return this.uploadZip<ImportConfirmResult>('/api/import/confirm', file);
  }

  private async uploadZip<T>(endpoint: string, file: File): Promise<T> {
    const response = await fetch(`${this.baseUrl}${endpoint}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/zip',
        'X-Filename': encodeURIComponent(file.name),
      },
      body: file,
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      const detail = errorData.detail;
      const message = typeof detail === 'string'
        ? detail
        : detail?.message || `HTTP ${response.status}: ${response.statusText}`;
      throw new Error(message);
    }

    return response.json();
  }

  // 获取群组专栏摘要信息
  async getGroupColumnsSummary(groupId: number | string): Promise<{
    has_columns: boolean;
    title: string | null;
    error?: string;
  }> {
    return this.request(`/api/groups/${groupId}/columns/summary`);
  }

  async getGroupTags(groupId: number | string): Promise<{ tags: GroupTag[]; total: number }> {
    return this.request(`/api/groups/${groupId}/tags`);
  }

  async getTagTopics(groupId: number | string, tagId: number, page: number = 1, perPage: number = 20): Promise<PaginatedResponse<Topic>> {
    const params = new URLSearchParams({
      page: page.toString(),
      per_page: perPage.toString(),
    });

    const response = await this.request<{topics: Topic[], pagination: any}>(`/api/groups/${groupId}/tags/${tagId}/topics?${params}`);
    return {
      data: response.topics,
      pagination: response.pagination,
    };
  }

  // 设置相关
  async getCrawlerSettings() {
    return this.request('/api/settings/crawler');
  }

  async updateCrawlerSettings(settings: {
    min_delay: number;
    max_delay: number;
    long_delay_interval: number;
    timestamp_offset_ms: number;
    debug_mode: boolean;
  }) {
    return this.request('/api/settings/crawler', {
      method: 'POST',
      body: JSON.stringify(settings),
    });
  }

  async getDownloaderSettings() {
    return this.request('/api/settings/downloader');
  }

  async updateDownloaderSettings(settings: {
    download_interval_min: number;
    download_interval_max: number;
    long_delay_interval: number;
    long_delay_min: number;
    long_delay_max: number;
  }) {
    return this.request('/api/settings/downloader', {
      method: 'POST',
      body: JSON.stringify(settings),
    });
  }

  async getCrawlSettings() {
    return this.request('/api/settings/crawl');
  }

  async updateCrawlSettings(settings: {
    crawl_interval_min: number;
    crawl_interval_max: number;
    long_sleep_interval_min: number;
    long_sleep_interval_max: number;
    pages_per_batch: number;
  }) {
    return this.request('/api/settings/crawl', {
      method: 'POST',
      body: JSON.stringify(settings),
    });
  }

  // 账号管理
  async listAccounts(): Promise<{ accounts: Account[] }> {
    return this.request('/api/accounts');
  }

  async createAccount(params: { cookie: string; name?: string }) {
    return this.request('/api/accounts', {
      method: 'POST',
      body: JSON.stringify({
        cookie: params.cookie,
        name: params.name,
      }),
    });
  }

  async deleteAccount(accountId: string) {
    return this.request(`/api/accounts/${accountId}`, {
      method: 'DELETE',
    });
  }

  async assignGroupAccount(groupId: number | string, accountId: string) {
    return this.request(`/api/groups/${groupId}/assign-account`, {
      method: 'POST',
      body: JSON.stringify({ account_id: accountId }),
    });
  }

  async getGroupAccount(groupId: number | string): Promise<{ account: Account | null }> {
    return this.request(`/api/groups/${groupId}/account`);
  }

  // 账号自我信息（/v3/users/self）
  async getAccountSelf(accountId: string): Promise<{ self: AccountSelf | null }> {
    return this.request(`/api/accounts/${accountId}/self`);
  }

  async refreshAccountSelf(accountId: string): Promise<{ self: AccountSelf | null }> {
    return this.request(`/api/accounts/${accountId}/self/refresh`, {
      method: 'POST',
    });
  }

  async getGroupAccountSelf(groupId: number | string): Promise<{ self: AccountSelf | null }> {
    return this.request(`/api/groups/${groupId}/self`);
  }

  async refreshGroupAccountSelf(groupId: number | string): Promise<{ self: AccountSelf | null }> {
    return this.request(`/api/groups/${groupId}/self/refresh`, {
      method: 'POST',
    });
  }
  async crawlByTimeRange(
    groupId: number | string,
    params: {
      startTime?: string;
      endTime?: string;
      lastDays?: number;
      perPage?: number;
      crawlIntervalMin?: number;
      crawlIntervalMax?: number;
      longSleepIntervalMin?: number;
      longSleepIntervalMax?: number;
      pagesPerBatch?: number;
    }
  ): Promise<TaskCreateResponse> {
    return this.request(`/api/crawl/range/${groupId}`, {
      method: 'POST',
      body: JSON.stringify(params || {}),
    });
  }
  // 删除社群本地数据
  async deleteGroup(groupId: number | string) {
    return this.request(`/api/groups/${groupId}`, {
      method: 'DELETE',
    });
  }

  async deleteAllLocalData() {
    return this.request('/api/local-data/reset', {
      method: 'DELETE',
    });
  }

  // =========================
  // 专栏相关 API
  // =========================

  // 获取群组专栏目录列表
  async getGroupColumns(groupId: number | string): Promise<{
    columns: ColumnInfo[];
    stats: ColumnsStats;
  }> {
    return this.request(`/api/groups/${groupId}/columns`);
  }

  // 获取专栏下的文章列表
  async getColumnTopics(groupId: number | string, columnId: number): Promise<{
    column: ColumnInfo;
    topics: ColumnTopic[];
  }> {
    return this.request(`/api/groups/${groupId}/columns/${columnId}/topics`);
  }

  // 获取专栏文章详情
  async getColumnTopicDetail(groupId: number | string, topicId: number): Promise<ColumnTopicDetail> {
    return this.request(`/api/groups/${groupId}/columns/topics/${topicId}`);
  }

  // 导出专栏文章为 Markdown：默认 zip（含 assets/ 头像与图片，离线可读）。
  getColumnTopicMarkdownExportUrl(
    groupId: number | string,
    topicId: number | string,
    format: 'zip' | 'md' = 'zip',
  ): string {
    const params = new URLSearchParams({ format });
    return `${this.baseUrl}/api/groups/${groupId}/columns/topics/${topicId}/export-md?${params.toString()}`;
  }

  async getColumnTopicFullComments(groupId: number | string, topicId: number): Promise<{
    success: boolean;
    comments: ColumnComment[];
    total: number;
  }> {
    return this.request(`/api/groups/${groupId}/columns/topics/${topicId}/comments`);
  }

  // 采集群组所有专栏内容
  async fetchGroupColumns(groupId: number | string, settings?: ColumnsFetchSettings): Promise<{
    success: boolean;
    task_id: string;
    message: string;
  }> {
    return this.request(`/api/groups/${groupId}/columns/fetch`, {
      method: 'POST',
      body: JSON.stringify(settings || {}),
    });
  }

  // 获取专栏统计信息
  async getColumnsStats(groupId: number | string): Promise<ColumnsStats> {
    return this.request(`/api/groups/${groupId}/columns/stats`);
  }

  // 删除群组所有专栏数据
  async deleteAllColumns(groupId: number | string): Promise<{
    success: boolean;
    message: string;
    deleted: {
      columns_deleted: number;
      topics_deleted: number;
      details_deleted: number;
      images_deleted: number;
      files_deleted: number;
      videos_deleted: number;
      comments_deleted: number;
    };
  }> {
    return this.request(`/api/groups/${groupId}/columns/all`, {
      method: 'DELETE',
    });
  }
}

// 专栏相关类型定义
export interface ColumnInfo {
  column_id: number;
  group_id: number;
  name: string;
  cover_url?: string;
  topics_count: number;
  create_time?: string;
  last_topic_attach_time?: string;
  imported_at?: string;
}

export interface ColumnTopic {
  topic_id: number;
  column_id: number;
  group_id: number;
  title?: string;
  text?: string;
  create_time?: string;
  attached_to_column_time?: string;
  imported_at?: string;
  has_detail?: boolean;
}

export interface ColumnTopicDetail {
  topic_id: number;
  group_id: number;
  type?: string;
  title?: string;
  full_text?: string;
  likes_count: number;
  comments_count: number;
  readers_count: number;
  digested: boolean;
  sticky: boolean;
  create_time?: string;
  modify_time?: string;
  raw_json?: string;
  imported_at?: string;
  updated_at?: string;
  owner?: {
    user_id: number;
    name: string;
    alias?: string;
    avatar_url?: string;
    description?: string;
    location?: string;
  };
  // Q&A type content
  question?: {
    text?: string;
    owner?: {
      user_id: number;
      name: string;
      alias?: string;
      avatar_url?: string;
    };
    images?: ColumnImage[];
  };
  answer?: {
    text?: string;
    owner?: {
      user_id: number;
      name: string;
      alias?: string;
      avatar_url?: string;
    };
    images?: ColumnImage[];
  };
  images: ColumnImage[];
  files: ColumnFile[];
  videos: ColumnVideo[];
  comments: ColumnComment[];
}

export interface ColumnImage {
  image_id: number;
  type?: string;
  thumbnail?: { url?: string; width?: number; height?: number };
  large?: { url?: string; width?: number; height?: number };
  original?: { url?: string; width?: number; height?: number; size?: number };
  local_path?: string;
}

export interface ColumnVideo {
  video_id: number;
  size?: number;
  duration?: number;
  cover?: {
    url?: string;
    width?: number;
    height?: number;
    local_path?: string;
  };
  video_url?: string;
  download_status?: string;
  local_path?: string;
  download_time?: string;
}

export interface ColumnFile {
  file_id: number;
  name: string;
  hash?: string;
  size?: number;
  duration?: number;
  download_count?: number;
  create_time?: string;
  download_status?: string;
  local_path?: string;
  download_time?: string;
}

export interface ColumnComment {
  comment_id: number;
  parent_comment_id?: number;
  text?: string;
  create_time?: string;
  likes_count: number;
  rewards_count: number;
  replies_count: number;
  sticky: boolean;
  owner?: {
    user_id: number;
    name: string;
    alias?: string;
    avatar_url?: string;
    location?: string;
  };
  repliee?: {
    user_id: number;
    name: string;
    alias?: string;
    avatar_url?: string;
  };
  images?: Array<{
    image_id?: number;
    type?: string;
    thumbnail?: { url?: string; width?: number; height?: number };
    large?: { url?: string; width?: number; height?: number };
    original?: { url?: string; width?: number; height?: number };
  }>;
  // Nested replies
  replied_comments?: ColumnComment[];
}

export interface ColumnsStats {
  columns_count: number;
  topics_count: number;
  details_count: number;
  images_count: number;
  files_count: number;
  files_downloaded: number;
  videos_count: number;
  videos_downloaded: number;
  comments_count: number;
}

export interface ColumnsFetchSettings {
  crawlIntervalMin?: number;
  crawlIntervalMax?: number;
  longSleepIntervalMin?: number;
  longSleepIntervalMax?: number;
  itemsPerBatch?: number;
  downloadFiles?: boolean;
  downloadVideos?: boolean;
  cacheImages?: boolean;
  incrementalMode?: boolean;
}

// 导出单例实例
export const apiClient = new ApiClient();
export default apiClient;
