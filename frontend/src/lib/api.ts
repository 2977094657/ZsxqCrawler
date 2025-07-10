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

  async updateConfig(config: {cookie: string, group_id: string, db_path?: string}) {
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

  async getTopicDetail(topicId: number, groupId: number) {
    return this.request(`/api/topics/${topicId}/${groupId}`);
  }

  async refreshTopic(topicId: number, groupId: number) {
    return this.request(`/api/topics/${topicId}/${groupId}/refresh`, {
      method: 'POST',
    });
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
  async getGroups(): Promise<{groups: Group[], total: number}> {
    return this.request('/api/groups');
  }

  async getGroupTopics(groupId: number, page: number = 1, perPage: number = 20, search?: string): Promise<PaginatedResponse<Topic>> {
    const params = new URLSearchParams({
      page: page.toString(),
      per_page: perPage.toString(),
    });

    if (search) {
      params.append('search', search);
    }

    const response = await this.request<{topics: Topic[], pagination: any}>(`/api/groups/${groupId}/topics?${params}`);
    return {
      data: response.topics,
      pagination: response.pagination,
    };
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
}

// 导出单例实例
export const apiClient = new ApiClient();
export default apiClient;
