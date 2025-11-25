/**
 * API客户端 - 与后端FastAPI服务通信
 */

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

// 类型定义
export interface ApiResponse<T = any> {
  data?: T;
  message?: string;
  error?: string;
}

export interface Task {
  task_id: string;
  type: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  message: string;
  result?: any;
  created_at: string;
  updated_at: string;
}

export interface DatabaseStats {
  configured?: boolean;
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
  async crawlHistorical(groupId: number, pages: number = 10, perPage: number = 20, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }) {
    return this.request(`/api/crawl/historical/${groupId}`, {
      method: 'POST',
      body: JSON.stringify({
        pages,
        per_page: perPage,
        ...crawlSettings
      }),
    });
  }

  async crawlAll(groupId: number, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }) {
    return this.request(`/api/crawl/all/${groupId}`, {
      method: 'POST',
      body: JSON.stringify(crawlSettings || {}),
    });
  }

  async crawlIncremental(groupId: number, pages: number = 10, perPage: number = 20, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }) {
    return this.request(`/api/crawl/incremental/${groupId}`, {
      method: 'POST',
      body: JSON.stringify({
        pages,
        per_page: perPage,
        ...crawlSettings
      }),
    });
  }

  async crawlLatestUntilComplete(groupId: number, crawlSettings?: {
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
    pagesPerBatch?: number;
  }) {
    return this.request(`/api/crawl/latest-until-complete/${groupId}`, {
      method: 'POST',
      body: JSON.stringify(crawlSettings || {}),
    });
  }

  async getTopicDetail(topicId: number | string, groupId: number) {
    // 统一转为字符串，避免大整数在前端被 Number 处理后精度丢失
    const id = String(topicId);
    return this.request(`/api/topics/${id}/${groupId}`);
  }

  async refreshTopic(topicId: number | string, groupId: number) {
    const id = String(topicId);
    return this.request(`/api/topics/${id}/${groupId}/refresh`, {
      method: 'POST',
    });
  }

  // 删除单个话题
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

  // 图片缓存管理
  async getImageCacheInfo(groupId: string) {
    return this.request(`/api/cache/images/info/${groupId}`);
  }

  async clearImageCache(groupId: string) {
    return this.request(`/api/cache/images/${groupId}`, {
      method: 'DELETE',
    });
  }

  // 群组相关
  async getGroupInfo(groupId: number) {
    return this.request(`/api/groups/${groupId}/info`);
  }

  // 文件相关
  async downloadFiles(groupId: number, maxFiles?: number, sortBy: string = 'download_count',
                     downloadInterval: number = 1.0, longSleepInterval: number = 60.0,
                     filesPerBatch: number = 10, downloadIntervalMin?: number,
                     downloadIntervalMax?: number, longSleepIntervalMin?: number,
                     longSleepIntervalMax?: number) {
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

  async clearFileDatabase(groupId: number) {
    return this.request(`/api/files/clear/${groupId}`, {
      method: 'POST',
    });
  }

  async clearTopicDatabase(groupId: number) {
    return this.request(`/api/topics/clear/${groupId}`, {
      method: 'POST',
    });
  }

  async getFileStats(groupId: number) {
    return this.request(`/api/files/stats/${groupId}`);
  }

  async downloadSingleFile(groupId: string, fileId: number, fileName?: string, fileSize?: number) {
    const params = new URLSearchParams();
    if (fileName) params.append('file_name', fileName);
    if (fileSize !== undefined) params.append('file_size', fileSize.toString());

    const url = `/api/files/download-single/${groupId}/${fileId}${params.toString() ? '?' + params.toString() : ''}`;
    return this.request(url, {
      method: 'POST',
    });
  }

  async getFileStatus(groupId: string, fileId: number) {
    return this.request(`/api/files/status/${groupId}/${fileId}`);
  }

  async checkLocalFileStatus(groupId: string, fileName: string, fileSize: number) {
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

  async getFiles(groupId: number, page: number = 1, perPage: number = 20, status?: string): Promise<PaginatedResponse<FileItem>> {
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

  async getGroupTopics(groupId: number, page: number = 1, perPage: number = 20, search?: string): Promise<PaginatedResponse<Topic>> {
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

  async getGroupStats(groupId: number): Promise<GroupStats> {
    return this.request(`/api/groups/${groupId}/stats`);
  }

  async getGroupTags(groupId: number) {
    return this.request(`/api/groups/${groupId}/tags`);
  }

  async getTagTopics(groupId: number, tagId: number, page: number = 1, perPage: number = 20): Promise<PaginatedResponse<Topic>> {
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

  async createAccount(params: { cookie: string; name?: string; make_default?: boolean }) {
    return this.request('/api/accounts', {
      method: 'POST',
      body: JSON.stringify({
        cookie: params.cookie,
        name: params.name,
        make_default: params.make_default ?? false,
      }),
    });
  }

  async deleteAccount(accountId: string) {
    return this.request(`/api/accounts/${accountId}`, {
      method: 'DELETE',
    });
  }

  async setDefaultAccount(accountId: string) {
    return this.request(`/api/accounts/${accountId}/default`, {
      method: 'POST',
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
    groupId: number,
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
  ) {
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
}

// 导出单例实例
export const apiClient = new ApiClient();
export default apiClient;
