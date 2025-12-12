'use client';

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { useParams, useRouter } from 'next/navigation';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog';
import { 
  ArrowLeft, BookOpen, FileText, Download, RefreshCw, 
  ChevronRight, File, FileImage, Clock, Heart, MessageCircle,
  Users, FolderOpen, Play, Settings, Trash2
} from 'lucide-react';
import { apiClient, ColumnInfo, ColumnTopic, ColumnTopicDetail, ColumnsStats, ColumnsFetchSettings } from '@/lib/api';
import { toast } from 'sonner';
import SafeImage from '@/components/SafeImage';
import ImageGallery from '@/components/ImageGallery';
import TaskLogViewer from '@/components/TaskLogViewer';
import { createSafeHtml } from '@/lib/zsxq-content-renderer';

export default function ColumnsPage() {
  const params = useParams();
  const router = useRouter();
  const groupId = params.groupId as string;

  // 状态
  const [columns, setColumns] = useState<ColumnInfo[]>([]);
  const [stats, setStats] = useState<ColumnsStats | null>(null);
  const [selectedColumn, setSelectedColumn] = useState<ColumnInfo | null>(null);
  const [columnTopics, setColumnTopics] = useState<ColumnTopic[]>([]);
  const [selectedTopic, setSelectedTopic] = useState<ColumnTopicDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [fetchingColumns, setFetchingColumns] = useState(false);
  const [currentTaskId, setCurrentTaskId] = useState<string | null>(null);
  const [loadingComments, setLoadingComments] = useState(false);

  // 采集设置
  const [settingsDialogOpen, setSettingsDialogOpen] = useState(false);
  const [crawlIntervalMin, setCrawlIntervalMin] = useState(2);
  const [crawlIntervalMax, setCrawlIntervalMax] = useState(5);
  const [longSleepIntervalMin, setLongSleepIntervalMin] = useState(30);
  const [longSleepIntervalMax, setLongSleepIntervalMax] = useState(60);
  const [itemsPerBatch, setItemsPerBatch] = useState(10);
  const [downloadFiles, setDownloadFiles] = useState(true);
  const [downloadVideos, setDownloadVideos] = useState(true);
  const [cacheImages, setCacheImages] = useState(true);
  const [incrementalMode, setIncrementalMode] = useState(true);  // 默认开启增量模式
  
  // 删除确认对话框
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);

  // 日志面板宽度（可拖拽调整）
  const [logPanelWidth, setLogPanelWidth] = useState(384); // 默认 w-96 = 384px
  const [isResizing, setIsResizing] = useState(false);
  const resizeRef = useRef<HTMLDivElement>(null);

  // 处理拖拽调整宽度
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsResizing(true);
  }, []);

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing) return;
      
      // 计算新宽度（从右边界开始）
      const newWidth = window.innerWidth - e.clientX;
      // 限制宽度范围：最小 280px，最大 800px
      const clampedWidth = Math.max(280, Math.min(800, newWidth));
      setLogPanelWidth(clampedWidth);
    };

    const handleMouseUp = () => {
      setIsResizing(false);
    };

    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
      // 拖拽时禁止选择文本
      document.body.style.userSelect = 'none';
      document.body.style.cursor = 'col-resize';
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
      document.body.style.userSelect = '';
      document.body.style.cursor = '';
    };
  }, [isResizing]);

  // 加载专栏目录
  const loadColumns = async () => {
    try {
      setLoading(true);
      const data = await apiClient.getGroupColumns(groupId);
      setColumns(data.columns || []);
      setStats(data.stats);
      
      // 如果有专栏，自动选择第一个
      if (data.columns && data.columns.length > 0) {
        setSelectedColumn(data.columns[0]);
        await loadColumnTopics(data.columns[0].column_id);
      }
    } catch (error) {
      console.error('加载专栏目录失败:', error);
      toast.error('加载专栏目录失败');
    } finally {
      setLoading(false);
    }
  };

  // 加载专栏文章列表
  const loadColumnTopics = async (columnId: number) => {
    try {
      setTopicsLoading(true);
      setSelectedTopic(null);
      const data = await apiClient.getColumnTopics(groupId, columnId);
      setColumnTopics(data.topics || []);
      
      // 如果有文章，自动选择第一个
      if (data.topics && data.topics.length > 0 && data.topics[0].has_detail) {
        await loadTopicDetail(data.topics[0].topic_id);
      }
    } catch (error) {
      console.error('加载专栏文章列表失败:', error);
      toast.error('加载文章列表失败');
    } finally {
      setTopicsLoading(false);
    }
  };

  // 加载文章详情
  const loadTopicDetail = async (topicId: number) => {
    try {
      setDetailLoading(true);
      const detail = await apiClient.getColumnTopicDetail(groupId, topicId);
      setSelectedTopic(detail);
    } catch (error) {
      console.error('加载文章详情失败:', error);
      toast.error('加载文章详情失败');
    } finally {
      setDetailLoading(false);
    }
  };

  // 获取完整评论
  const handleFetchMoreComments = async () => {
    if (!selectedTopic) return;

    try {
      setLoadingComments(true);
      const result = await apiClient.getColumnTopicFullComments(groupId, selectedTopic.topic_id);
      if (result.success && result.comments) {
        // 更新当前选中文章的评论
        setSelectedTopic(prev => prev ? {
          ...prev,
          comments: result.comments
        } : null);
        toast.success(`已获取 ${result.total} 条评论`);
      }
    } catch (error) {
      console.error('获取完整评论失败:', error);
      toast.error('获取完整评论失败');
    } finally {
      setLoadingComments(false);
    }
  };

  // 采集所有专栏
  const handleFetchColumns = async () => {
    try {
      setFetchingColumns(true);
      setSettingsDialogOpen(false);
      
      const settings: ColumnsFetchSettings = {
        crawlIntervalMin,
        crawlIntervalMax,
        longSleepIntervalMin,
        longSleepIntervalMax,
        itemsPerBatch,
        downloadFiles,
        downloadVideos,
        cacheImages,
        incrementalMode,
      };
      
      const result = await apiClient.fetchGroupColumns(groupId, settings);
      if (result.success) {
        setCurrentTaskId(result.task_id);
        toast.success(incrementalMode ? '增量采集任务已启动（跳过已存在）' : '全量采集任务已启动');
      }
    } catch (error) {
      console.error('启动专栏采集失败:', error);
      toast.error('启动专栏采集失败');
      setFetchingColumns(false);
    }
  };

  // 删除所有专栏数据
  const handleDeleteAllColumns = async () => {
    try {
      setDeleting(true);
      const result = await apiClient.deleteAllColumns(groupId);
      if (result.success) {
        toast.success(`已清空专栏数据：删除 ${result.deleted.columns_deleted} 个专栏，${result.deleted.details_deleted} 篇文章`);
        // 重新加载
        setColumns([]);
        setColumnTopics([]);
        setSelectedColumn(null);
        setSelectedTopic(null);
        setStats(null);
        await loadColumns();
      }
    } catch (error) {
      console.error('删除专栏数据失败:', error);
      toast.error('删除专栏数据失败');
    } finally {
      setDeleting(false);
      setDeleteDialogOpen(false);
    }
  };

  // 选择专栏
  const handleSelectColumn = async (column: ColumnInfo) => {
    setSelectedColumn(column);
    await loadColumnTopics(column.column_id);
  };

  // 选择文章
  const handleSelectTopic = async (topic: ColumnTopic) => {
    if (topic.has_detail) {
      await loadTopicDetail(topic.topic_id);
    } else {
      toast.info('该文章尚未采集详情，请先采集专栏内容');
    }
  };

  // 格式化文件大小
  const formatFileSize = (bytes?: number): string => {
    if (!bytes) return '未知';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  };

  // 格式化视频时长
  const formatDuration = (seconds?: number): string => {
    if (!seconds) return '未知';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    if (hours > 0) {
      return `${hours}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    }
    return `${minutes}:${secs.toString().padStart(2, '0')}`;
  };

  // 格式化时间
  const formatTime = (time?: string): string => {
    if (!time) return '';
    try {
      const date = new Date(time);
      return date.toLocaleDateString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
      });
    } catch {
      return time;
    }
  };

  useEffect(() => {
    loadColumns();
  }, [groupId]);

  // 渲染导航栏内的紧凑统计信息
  const renderNavStats = () => {
    if (!stats) return null;
    
    return (
      <div className="flex items-center gap-3 text-xs">
        <span className="text-blue-600 font-medium">{stats.columns_count} 专栏</span>
        <span className="text-gray-300">|</span>
        <span className="text-green-600 font-medium">{stats.details_count} 文章</span>
        <span className="text-gray-300">|</span>
        <span className="text-purple-600 font-medium">{stats.files_downloaded}/{stats.files_count} 文件</span>
        <span className="text-gray-300">|</span>
        <span className="text-rose-600 font-medium">{stats.videos_downloaded}/{stats.videos_count} 视频</span>
        <span className="text-gray-300">|</span>
        <span className="text-orange-600 font-medium">{stats.images_count} 图片</span>
      </div>
    );
  };

  // 渲染专栏目录列表
  const renderColumnList = () => {
    return (
      <div className="space-y-1">
        {columns.map((column) => (
          <button
            key={column.column_id}
            onClick={() => handleSelectColumn(column)}
            className={`w-full text-left px-3 py-2 rounded-lg transition-colors flex items-center justify-between ${
              selectedColumn?.column_id === column.column_id
                ? 'bg-amber-100 text-amber-800 border border-amber-200'
                : 'hover:bg-gray-100 text-gray-700'
            }`}
          >
            <div className="flex items-center gap-2 min-w-0">
              <FolderOpen className="h-4 w-4 flex-shrink-0" />
              <span className="truncate text-sm">{column.name}</span>
            </div>
            <Badge variant="secondary" className="flex-shrink-0 text-xs">
              {column.topics_count}
            </Badge>
          </button>
        ))}
      </div>
    );
  };

  // 渲染文章列表
  const renderTopicList = () => {
    if (topicsLoading) {
      return <div className="text-center text-gray-500 py-8">加载中...</div>;
    }

    if (columnTopics.length === 0) {
      return (
        <div className="text-center text-gray-500 py-8">
          暂无文章，请先采集专栏内容
        </div>
      );
    }

    return (
      <div className="space-y-1">
        {columnTopics.map((topic) => (
          <button
            key={topic.topic_id}
            onClick={() => handleSelectTopic(topic)}
            className={`w-full text-left px-3 py-2 rounded-lg transition-colors ${
              selectedTopic?.topic_id === topic.topic_id
                ? 'bg-blue-50 text-blue-800 border border-blue-200'
                : topic.has_detail
                  ? 'hover:bg-gray-50 text-gray-700'
                  : 'hover:bg-gray-50 text-gray-400'
            }`}
          >
            <div className="flex items-start gap-2">
              <FileText className={`h-4 w-4 mt-0.5 flex-shrink-0 ${
                topic.has_detail ? '' : 'opacity-50'
              }`} />
              <div className="min-w-0 flex-1">
                <div className={`text-sm font-medium truncate ${
                  topic.has_detail ? '' : 'opacity-50'
                }`}>
                  {topic.title || '无标题'}
                </div>
                <div className="text-xs text-gray-400 mt-0.5">
                  {formatTime(topic.create_time)}
                </div>
              </div>
              {!topic.has_detail && (
                <Badge variant="outline" className="text-xs flex-shrink-0 opacity-50">
                  未采集
                </Badge>
              )}
            </div>
          </button>
        ))}
      </div>
    );
  };

  // 渲染文章详情
  const renderTopicDetail = () => {
    if (detailLoading) {
      return (
        <div className="flex items-center justify-center h-full">
          <div className="text-gray-500">加载中...</div>
        </div>
      );
    }

    if (!selectedTopic) {
      return (
        <div className="flex items-center justify-center h-full">
          <div className="text-center text-gray-400">
            <BookOpen className="h-12 w-12 mx-auto mb-4 opacity-50" />
            <p>选择一篇文章查看详情</p>
          </div>
        </div>
      );
    }

    return (
      <ScrollArea className="h-full">
        <div className="p-6 max-w-3xl mx-auto">
          {/* 标题 */}
          <h1 className="text-2xl font-bold text-gray-900 mb-4">
            {selectedTopic.title || '无标题'}
          </h1>

          {/* 作者和时间 */}
          <div className="flex items-center gap-4 mb-6 pb-4 border-b border-gray-200">
            {selectedTopic.owner && (
              <div className="flex items-center gap-2">
                <img
                  src={apiClient.getProxyImageUrl(selectedTopic.owner.avatar_url || '', groupId)}
                  alt={selectedTopic.owner.name}
                  className="w-8 h-8 rounded-full object-cover"
                  onError={(e) => {
                    e.currentTarget.src = '/default-avatar.png';
                  }}
                />
                <span className="text-sm font-medium text-gray-700">
                  {selectedTopic.owner.name}
                </span>
              </div>
            )}
            <div className="flex items-center gap-4 text-sm text-gray-500">
              <span className="flex items-center gap-1">
                <Clock className="h-4 w-4" />
                {formatTime(selectedTopic.create_time)}
              </span>
              <span className="flex items-center gap-1">
                <Heart className="h-4 w-4" />
                {selectedTopic.likes_count}
              </span>
              <span className="flex items-center gap-1">
                <MessageCircle className="h-4 w-4" />
                {selectedTopic.comments_count}
              </span>
              <span className="flex items-center gap-1">
                <Users className="h-4 w-4" />
                {selectedTopic.readers_count}
              </span>
            </div>
          </div>

          {/* Q&A 类型内容 - 与普通话题样式一致 */}
          {selectedTopic.type === 'q&a' && selectedTopic.question && (
            <div className="mb-6 space-y-4">
              {/* 提问部分 */}
              <div className="w-full max-w-full overflow-hidden">
                {/* 提问者信息 */}
                <div className="text-sm text-gray-600 mb-2">
                  <span className="font-medium">
                    {selectedTopic.question.owner?.name || '用户'} 提问：
                  </span>
                </div>

                {/* 问题内容 - 使用引用样式 */}
                <div className="bg-gray-50 border-l-4 border-gray-300 pl-4 py-3 rounded-r-lg w-full max-w-full overflow-hidden">
                  {selectedTopic.question.text && (
                    <div
                      className="text-sm text-gray-500 whitespace-pre-wrap break-words leading-relaxed prose prose-sm max-w-none prose-p:my-1 prose-a:text-blue-500"
                      dangerouslySetInnerHTML={createSafeHtml(selectedTopic.question.text)}
                    />
                  )}
                  {/* 提问图片 */}
                  {selectedTopic.question.images && selectedTopic.question.images.length > 0 && (
                    <div className="mt-3">
                      <ImageGallery
                        images={selectedTopic.question.images}
                        size="small"
                        groupId={groupId}
                      />
                    </div>
                  )}
                </div>
              </div>

              {/* 回答部分 - 直接显示内容，不使用特殊框 */}
              {selectedTopic.answer && selectedTopic.answer.text && (
                <div className="w-full max-w-full overflow-hidden">
                  <div
                    className="text-sm text-gray-800 whitespace-pre-wrap break-words leading-relaxed prose prose-sm max-w-none prose-p:my-1 prose-strong:text-gray-900 prose-a:text-blue-600"
                    dangerouslySetInnerHTML={createSafeHtml(selectedTopic.answer.text)}
                  />
                  {/* 回答图片 */}
                  {selectedTopic.answer.images && selectedTopic.answer.images.length > 0 && (
                    <div className="mt-3">
                      <ImageGallery
                        images={selectedTopic.answer.images}
                        size="small"
                        groupId={groupId}
                      />
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {/* 正文内容（非Q&A类型或作为补充内容） */}
          {selectedTopic.full_text && selectedTopic.type !== 'q&a' && (
            <div 
              className="prose prose-gray max-w-none mb-6"
              dangerouslySetInnerHTML={createSafeHtml(selectedTopic.full_text)}
            />
          )}

          {/* 图片列表 */}
          {selectedTopic.images && selectedTopic.images.length > 0 && (
            <div className="mb-6">
              <h3 className="text-lg font-semibold text-gray-800 mb-3 flex items-center gap-2">
                <FileImage className="h-5 w-5" />
                图片 ({selectedTopic.images.length})
              </h3>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                {selectedTopic.images.map((image) => (
                  <a
                    key={image.image_id}
                    href={apiClient.getProxyImageUrl(image.original?.url || '', groupId)}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="block aspect-video rounded-lg overflow-hidden bg-gray-100 hover:opacity-80 transition-opacity"
                  >
                    <img
                      src={apiClient.getProxyImageUrl(image.large?.url || image.thumbnail?.url || '', groupId)}
                      alt=""
                      className="w-full h-full object-cover"
                      loading="lazy"
                    />
                  </a>
                ))}
              </div>
            </div>
          )}

          {/* 文件列表 */}
          {selectedTopic.files && selectedTopic.files.length > 0 && (
            <div className="mb-6">
              <h3 className="text-lg font-semibold text-gray-800 mb-3 flex items-center gap-2">
                <File className="h-5 w-5" />
                文件 ({selectedTopic.files.length})
              </h3>
              <div className="space-y-2">
                {selectedTopic.files.map((file) => (
                  <div
                    key={file.file_id}
                    className={`flex items-center gap-3 p-3 rounded-lg border ${
                      file.download_status === 'completed'
                        ? 'bg-green-50 border-green-200'
                        : 'bg-gray-50 border-gray-200'
                    }`}
                  >
                    <File className="h-5 w-5 text-gray-500 flex-shrink-0" />
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium text-gray-900 truncate">
                        {file.name}
                      </div>
                      <div className="text-xs text-gray-500">
                        {formatFileSize(file.size)}
                        {file.download_status === 'completed' && (
                          <span className="text-green-600 ml-2">✓ 已下载</span>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* 视频列表 */}
          {selectedTopic.videos && selectedTopic.videos.length > 0 && (
            <div className="mb-6">
              <h3 className="text-lg font-semibold text-gray-800 mb-3 flex items-center gap-2">
                <Play className="h-5 w-5" />
                视频 ({selectedTopic.videos.length})
              </h3>
              <div className="space-y-3">
                {selectedTopic.videos.map((video) => (
                  <div
                    key={video.video_id}
                    className={`rounded-lg border overflow-hidden ${
                      video.download_status === 'completed'
                        ? 'border-green-200 bg-green-50'
                        : 'border-gray-200 bg-gray-50'
                    }`}
                  >
                    {/* 视频播放器或封面 */}
                    <div className="relative aspect-video bg-black">
                      {video.download_status === 'completed' && video.local_path ? (
                        // 已下载：显示视频播放器
                        <video
                          controls
                          className="w-full h-full"
                          poster={video.cover?.local_path 
                            ? apiClient.getLocalImageUrl(groupId, video.cover.local_path)
                            : video.cover?.url 
                              ? apiClient.getProxyImageUrl(video.cover.url, groupId)
                              : undefined
                          }
                        >
                          <source 
                            src={apiClient.getLocalVideoUrl(groupId, `video_${video.video_id}.mp4`)} 
                            type="video/mp4" 
                          />
                          Your browser does not support the video tag.
                        </video>
                      ) : (
                        // 未下载：显示封面和状态
                        <>
                          {video.cover?.url && (
                            <img
                              src={video.cover.local_path 
                                ? apiClient.getLocalImageUrl(groupId, video.cover.local_path)
                                : apiClient.getProxyImageUrl(video.cover.url, groupId)
                              }
                              alt="视频封面"
                              className="w-full h-full object-contain"
                            />
                          )}
                          <div className="absolute inset-0 flex items-center justify-center">
                            <div className="text-center">
                              <div className="w-16 h-16 mx-auto rounded-full bg-black/50 flex items-center justify-center mb-2">
                                <Play className="h-8 w-8 text-white/50 ml-1" fill="white" fillOpacity={0.5} />
                              </div>
                              <span className="text-white/80 text-sm bg-black/50 px-3 py-1 rounded">
                                {video.download_status === 'pending' && '等待下载'}
                                {video.download_status === 'pending_manual' && '需手动下载'}
                                {video.download_status === 'failed' && '下载失败'}
                                {video.download_status === 'downloading' && '下载中...'}
                                {!video.download_status && '未下载'}
                              </span>
                            </div>
                          </div>
                        </>
                      )}
                    </div>
                    <div className="p-3">
                      <div className="flex items-center gap-4 text-sm text-gray-600">
                        <span>大小: {formatFileSize(video.size)}</span>
                        <span>时长: {formatDuration(video.duration)}</span>
                        <span className={
                          video.download_status === 'completed' ? 'text-green-600' :
                          video.download_status === 'pending_manual' ? 'text-yellow-600' :
                          video.download_status === 'failed' ? 'text-red-600' :
                          'text-gray-500'
                        }>
                          {video.download_status === 'completed' && '✓ 已下载，可播放'}
                          {video.download_status === 'pending' && '待下载'}
                          {video.download_status === 'pending_manual' && '需手动下载'}
                          {video.download_status === 'failed' && '下载失败'}
                          {video.download_status === 'downloading' && '下载中...'}
                        </span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* 评论列表 */}
          {(selectedTopic.comments && selectedTopic.comments.length > 0) || selectedTopic.comments_count > 0 ? (
            <div>
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-lg font-semibold text-gray-800 flex items-center gap-2">
                  <MessageCircle className="h-5 w-5" />
                  评论 ({(() => {
                    // 计算总评论数（包括嵌套回复）
                    let total = 0;
                    selectedTopic.comments?.forEach(c => {
                      total += 1;
                      if (c.replied_comments) {
                        total += c.replied_comments.length;
                      }
                    });
                    return total;
                  })()}/{selectedTopic.comments_count})
                </h3>
                {/* 获取更多评论按钮 */}
                {selectedTopic.comments_count > (() => {
                  let total = 0;
                  selectedTopic.comments?.forEach(c => {
                    total += 1;
                    if (c.replied_comments) {
                      total += c.replied_comments.length;
                    }
                  });
                  return total;
                })() && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleFetchMoreComments}
                    disabled={loadingComments}
                    className="text-xs"
                  >
                    <RefreshCw className={`h-3 w-3 mr-1 ${loadingComments ? 'animate-spin' : ''}`} />
                    {loadingComments ? '获取中...' : '获取完整评论'}
                  </Button>
                )}
              </div>
              <div className="space-y-2">
                {selectedTopic.comments?.map((comment) => (
                  <div key={comment.comment_id} className="bg-gray-50 rounded-lg p-2">
                    <div className="flex items-center gap-2 mb-1">
                      {comment.owner && (
                        <img
                          src={apiClient.getProxyImageUrl(comment.owner.avatar_url || '', groupId)}
                          alt={comment.owner.name}
                          loading="lazy"
                          decoding="async"
                          className="w-4 h-4 rounded-full object-cover block"
                          onError={(e) => {
                            (e.currentTarget as HTMLImageElement).style.display = 'none';
                          }}
                        />
                      )}
                      <span className="text-xs font-medium text-gray-700">
                        {comment.owner?.name || '未知用户'}
                      </span>
                      {/* 显示回复关系 */}
                      {comment.repliee && (
                        <>
                          <span className="text-xs text-gray-400">回复</span>
                          <span className="text-xs font-medium text-blue-600">
                            {comment.repliee.name}
                          </span>
                        </>
                      )}
                      <span className="text-xs text-gray-500">
                        {formatTime(comment.create_time)}
                      </span>
                    </div>
                    <div
                      className="text-xs text-gray-600 ml-6 break-words prose prose-xs max-w-none prose-a:text-blue-600"
                      dangerouslySetInnerHTML={createSafeHtml(comment.text || '')}
                    />

                    {/* 评论图片 */}
                    {comment.images && comment.images.length > 0 && (
                      <div className="ml-6 mt-2">
                        <ImageGallery
                          images={comment.images}
                          className="comment-images"
                          size="small"
                          groupId={groupId}
                        />
                      </div>
                    )}

                    {/* 嵌套回复评论 */}
                    {comment.replied_comments && comment.replied_comments.length > 0 && (
                      <div className="ml-6 mt-2 space-y-2 border-l-2 border-gray-200 pl-3">
                        {comment.replied_comments.map((reply) => (
                          <div key={reply.comment_id} className="bg-white rounded p-2">
                            <div className="flex items-center gap-2 mb-1">
                              {reply.owner && (
                                <img
                                  src={apiClient.getProxyImageUrl(reply.owner.avatar_url || '', groupId)}
                                  alt={reply.owner.name}
                                  loading="lazy"
                                  decoding="async"
                                  className="w-3 h-3 rounded-full object-cover block"
                                  onError={(e) => {
                                    (e.currentTarget as HTMLImageElement).style.display = 'none';
                                  }}
                                />
                              )}
                              <span className="text-xs font-medium text-gray-600">
                                {reply.owner?.name || '未知用户'}
                              </span>
                              {reply.repliee && (
                                <>
                                  <span className="text-xs text-gray-400">回复</span>
                                  <span className="text-xs font-medium text-blue-500">
                                    {reply.repliee.name}
                                  </span>
                                </>
                              )}
                              <span className="text-xs text-gray-400">
                                {formatTime(reply.create_time)}
                              </span>
                            </div>
                            <div
                              className="text-xs text-gray-500 ml-5 break-words prose prose-xs max-w-none prose-a:text-blue-600"
                              dangerouslySetInnerHTML={createSafeHtml(reply.text || '')}
                            />
                            {/* 嵌套回复图片 */}
                            {reply.images && reply.images.length > 0 && (
                              <div className="ml-5 mt-1">
                                <ImageGallery
                                  images={reply.images}
                                  className="reply-images"
                                  size="small"
                                  groupId={groupId}
                                />
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          ) : null}
        </div>
      </ScrollArea>
    );
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-gray-500">加载中...</div>
      </div>
    );
  }

  return (
    <div className="h-screen bg-gray-50 flex flex-col overflow-hidden">
      {/* 顶部导航栏 */}
      <div className="flex-shrink-0 p-4 bg-white border-b border-gray-200">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Button
              variant="ghost"
              onClick={() => router.push(`/groups/${groupId}`)}
              className="flex items-center gap-2"
            >
              <ArrowLeft className="h-4 w-4" />
              返回群组
            </Button>
            <div className="flex items-center gap-2">
              <BookOpen className="h-5 w-5 text-amber-600" />
              <h1 className="text-lg font-semibold text-gray-900">专栏课程</h1>
            </div>
            
            {/* 导航栏中的统计信息 */}
            {renderNavStats()}
          </div>
          
          <div className="flex items-center gap-3">
            <Button
              variant="outline"
              size="sm"
              onClick={loadColumns}
              className="flex items-center gap-2"
            >
              <RefreshCw className="h-4 w-4" />
              刷新
            </Button>
            
            {/* 删除专栏确认对话框 */}
            <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
              <DialogTrigger asChild>
                <Button
                  variant="outline"
                  size="sm"
                  className="flex items-center gap-2 text-red-600 hover:text-red-700 hover:bg-red-50 border-red-200"
                  disabled={deleting || !stats || stats.columns_count === 0}
                >
                  <Trash2 className="h-4 w-4" />
                  清空数据
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle className="text-red-600">确认删除所有专栏数据</DialogTitle>
                  <DialogDescription asChild>
                    <div>
                      <span>此操作将删除该群组的所有专栏数据，包括：</span>
                      <ul className="mt-2 space-y-1 text-sm">
                        <li>• {stats?.columns_count || 0} 个专栏目录</li>
                        <li>• {stats?.topics_count || 0} 篇文章列表</li>
                        <li>• {stats?.details_count || 0} 篇文章详情</li>
                        <li>• {stats?.files_count || 0} 个文件记录</li>
                        <li>• {stats?.videos_count || 0} 个视频记录</li>
                        <li>• {stats?.images_count || 0} 张图片记录</li>
                      </ul>
                      <div className="mt-3 text-red-500 font-medium">
                        删除后可重新采集，但本地已下载的文件不会被删除。
                      </div>
                    </div>
                  </DialogDescription>
                </DialogHeader>
                <DialogFooter>
                  <Button variant="outline" onClick={() => setDeleteDialogOpen(false)}>
                    取消
                  </Button>
                  <Button 
                    variant="destructive"
                    onClick={handleDeleteAllColumns}
                    disabled={deleting}
                  >
                    {deleting ? '删除中...' : '确认删除'}
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
            
            {/* 采集设置对话框 */}
            <Dialog open={settingsDialogOpen} onOpenChange={setSettingsDialogOpen}>
              <DialogTrigger asChild>
                <Button
                  disabled={fetchingColumns}
                  className="flex items-center gap-2 bg-amber-500 hover:bg-amber-600 text-white"
                >
                  <Download className="h-4 w-4" />
                  {fetchingColumns ? '采集中...' : '采集全部专栏'}
                </Button>
              </DialogTrigger>
              <DialogContent className="sm:max-w-[500px]">
                <DialogHeader>
                  <DialogTitle className="flex items-center gap-2">
                    <Settings className="h-5 w-5" />
                    专栏采集设置
                  </DialogTitle>
                  <DialogDescription>
                    配置专栏采集的间隔参数，避免请求过于频繁
                  </DialogDescription>
                </DialogHeader>
                
                <div className="space-y-6 py-4">
                  {/* 请求间隔 */}
                  <div className="space-y-3">
                    <Label className="text-sm font-medium">请求间隔 (秒)</Label>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label className="text-xs text-gray-500">最小</Label>
                        <Input
                          type="number"
                          min={1}
                          max={60}
                          step={0.5}
                          value={crawlIntervalMin}
                          onChange={(e) => setCrawlIntervalMin(parseFloat(e.target.value) || 2)}
                        />
                      </div>
                      <div>
                        <Label className="text-xs text-gray-500">最大</Label>
                        <Input
                          type="number"
                          min={1}
                          max={60}
                          step={0.5}
                          value={crawlIntervalMax}
                          onChange={(e) => setCrawlIntervalMax(parseFloat(e.target.value) || 5)}
                        />
                      </div>
                    </div>
                  </div>
                  
                  {/* 长休眠间隔 */}
                  <div className="space-y-3">
                    <Label className="text-sm font-medium">长休眠间隔 (秒)</Label>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label className="text-xs text-gray-500">最小</Label>
                        <Input
                          type="number"
                          min={10}
                          max={600}
                          step={5}
                          value={longSleepIntervalMin}
                          onChange={(e) => setLongSleepIntervalMin(parseFloat(e.target.value) || 30)}
                        />
                      </div>
                      <div>
                        <Label className="text-xs text-gray-500">最大</Label>
                        <Input
                          type="number"
                          min={10}
                          max={600}
                          step={5}
                          value={longSleepIntervalMax}
                          onChange={(e) => setLongSleepIntervalMax(parseFloat(e.target.value) || 60)}
                        />
                      </div>
                    </div>
                  </div>
                  
                  {/* 批次大小 */}
                  <div className="space-y-3">
                    <Label className="text-sm font-medium">每批次请求数</Label>
                    <Input
                      type="number"
                      min={3}
                      max={50}
                      value={itemsPerBatch}
                      onChange={(e) => setItemsPerBatch(parseInt(e.target.value) || 10)}
                    />
                    <p className="text-xs text-gray-500">
                      每完成指定数量的请求后，会进入长休眠
                    </p>
                  </div>
                  
                  {/* 文件和图片选项 */}
                  <div className="space-y-3">
                    <Label className="text-sm font-medium">附件选项</Label>
                    <div className="flex items-center gap-6">
                      <label className="flex items-center gap-2 cursor-pointer">
                        <input
                          type="checkbox"
                          checked={downloadFiles}
                          onChange={(e) => setDownloadFiles(e.target.checked)}
                          className="w-4 h-4 rounded border-gray-300"
                        />
                        <span className="text-sm">下载文件</span>
                      </label>
                      <label className="flex items-center gap-2 cursor-pointer">
                        <input
                          type="checkbox"
                          checked={downloadVideos}
                          onChange={(e) => setDownloadVideos(e.target.checked)}
                          className="w-4 h-4 rounded border-gray-300"
                        />
                        <span className="text-sm">下载视频</span>
                        <span className="text-xs text-gray-400">(需ffmpeg)</span>
                      </label>
                      <label className="flex items-center gap-2 cursor-pointer">
                        <input
                          type="checkbox"
                          checked={cacheImages}
                          onChange={(e) => setCacheImages(e.target.checked)}
                          className="w-4 h-4 rounded border-gray-300"
                        />
                        <span className="text-sm">缓存图片</span>
                      </label>
                    </div>
                  </div>
                </div>
                
                {/* 增量模式 */}
                <div className="p-4 bg-blue-50 rounded-lg border border-blue-200">
                  <label className="flex items-center gap-3 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={incrementalMode}
                      onChange={(e) => setIncrementalMode(e.target.checked)}
                      className="w-5 h-5 rounded border-gray-300"
                    />
                    <div>
                      <span className="text-sm font-medium text-blue-800">增量采集模式</span>
                      <p className="text-xs text-blue-600 mt-0.5">
                        开启后将跳过已采集的文章，只获取新内容（推荐）
                      </p>
                    </div>
                  </label>
                </div>
                
                <DialogFooter>
                  <Button variant="outline" onClick={() => setSettingsDialogOpen(false)}>
                    取消
                  </Button>
                  <Button 
                    onClick={handleFetchColumns}
                    className="bg-amber-500 hover:bg-amber-600 text-white"
                  >
                    <Download className="h-4 w-4 mr-2" />
                    {incrementalMode ? '增量采集' : '全量采集'}
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </div>
        </div>
      </div>

      {/* 主体内容 - 动态布局 */}
      <div className="flex-1 flex min-h-0">
        {/* 左侧：专栏目录 */}
        <div className="w-64 flex-shrink-0 border-r border-gray-200 bg-white">
          <div className="h-full flex flex-col">
            <div className="p-4 border-b border-gray-200">
              <h2 className="text-sm font-semibold text-gray-700 flex items-center gap-2">
                <FolderOpen className="h-4 w-4" />
                专栏目录
              </h2>
            </div>
            <ScrollArea className="flex-1">
              <div className="p-2">
                {columns.length === 0 ? (
                  <div className="text-center text-gray-400 py-8 text-sm">
                    暂无专栏数据
                    <br />
                    请先采集专栏内容
                  </div>
                ) : (
                  renderColumnList()
                )}
              </div>
            </ScrollArea>
          </div>
        </div>

        {/* 中间：文章列表 */}
        <div className="w-80 flex-shrink-0 border-r border-gray-200 bg-white">
          <div className="h-full flex flex-col">
            <div className="p-4 border-b border-gray-200">
              <h2 className="text-sm font-semibold text-gray-700 flex items-center gap-2">
                <FileText className="h-4 w-4" />
                {selectedColumn?.name || '文章列表'}
                {selectedColumn && (
                  <Badge variant="secondary" className="text-xs">
                    {columnTopics.length}
                  </Badge>
                )}
              </h2>
            </div>
            <ScrollArea className="flex-1">
              <div className="p-2">
                {!selectedColumn ? (
                  <div className="text-center text-gray-400 py-8 text-sm">
                    请选择一个专栏
                  </div>
                ) : (
                  renderTopicList()
                )}
              </div>
            </ScrollArea>
          </div>
        </div>

        {/* 文章详情区域 */}
        <div className="flex-1 bg-white min-w-0">
          {renderTopicDetail()}
        </div>

        {/* 右侧：任务日志面板 - inline 模式（可拖拽调整宽度） */}
        {currentTaskId && (
          <>
            {/* 拖拽分隔条 */}
            <div
              ref={resizeRef}
              onMouseDown={handleMouseDown}
              className={`w-1 flex-shrink-0 cursor-col-resize hover:bg-blue-400 transition-colors ${
                isResizing ? 'bg-blue-500' : 'bg-gray-300'
              }`}
              title="拖拽调整宽度"
            />
            {/* 日志面板 */}
            <div 
              className="flex-shrink-0 border-l border-gray-200 bg-gradient-to-br from-slate-50 to-gray-100"
              style={{ width: logPanelWidth }}
            >
              <TaskLogViewer
                taskId={currentTaskId}
                onClose={() => {
                  setCurrentTaskId(null);
                  setFetchingColumns(false);
                }}
                inline={true}
                onTaskStop={() => {
                  // 任务停止时清除任务ID，避免重复触发
                  setCurrentTaskId(null);
                  setFetchingColumns(false);
                  loadColumns();
                }}
              />
            </div>
          </>
        )}
      </div>
    </div>
  );
}

