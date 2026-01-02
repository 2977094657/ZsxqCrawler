'use client';

import { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { apiClient, Topic, FileItem, PaginatedResponse, Group } from '@/lib/api';

interface DataPanelProps {
  selectedGroup?: Group | null;
}

export default function DataPanel({ selectedGroup }: DataPanelProps) {
  const [topicsData, setTopicsData] = useState<PaginatedResponse<Topic> | null>(null);
  const [filesData, setFilesData] = useState<PaginatedResponse<FileItem> | null>(null);
  const [loading, setLoading] = useState(false);

  const selectedGroupId = selectedGroup?.group_id;
  
  // 话题查询参数
  const [topicsPage, setTopicsPage] = useState(1);
  const [topicsSearch, setTopicsSearch] = useState('');
  const [topicsSearchQuery, setTopicsSearchQuery] = useState('');
  
  // 文件查询参数
  const [filesPage, setFilesPage] = useState(1);
  const [filesStatus, setFilesStatus] = useState<string>('');

  useEffect(() => {
    setTopicsPage(1);
    setFilesPage(1);
  }, [selectedGroupId]);

  useEffect(() => {
    const loadTopics = async () => {
      try {
        setLoading(true);
        let data;
        if (selectedGroupId !== undefined) {
          data = await apiClient.getGroupTopics(selectedGroupId, topicsPage, 20, topicsSearchQuery || undefined);
        } else {
          data = await apiClient.getTopics(topicsPage, 20, topicsSearchQuery || undefined);
        }
        setTopicsData(data);
      } catch (error) {
        console.error('加载话题数据失败:', error);
      } finally {
        setLoading(false);
      }
    };

    loadTopics();
  }, [selectedGroupId, topicsPage, topicsSearchQuery]);

  useEffect(() => {
    const loadFiles = async () => {
      if (selectedGroupId === undefined) {
        setFilesData(null);
        return;
      }

      try {
        setLoading(true);
        const data = await apiClient.getFiles(selectedGroupId, filesPage, 20, filesStatus || undefined);
        setFilesData(data);
      } catch (error) {
        console.error('加载文件数据失败:', error);
      } finally {
        setLoading(false);
      }
    };

    loadFiles();
  }, [selectedGroupId, filesPage, filesStatus]);

  const handleTopicsSearch = () => {
    setTopicsPage(1);
    setTopicsSearchQuery(topicsSearch.trim());
  };

  const handleFilesStatusChange = (value: string) => {
    setFilesPage(1);
    setFilesStatus(value);
  };

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatDate = (dateString: string) => {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleString('zh-CN');
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'completed':
      case 'downloaded':
        return <Badge className="bg-green-100 text-green-800">已完成</Badge>;
      case 'pending':
        return <Badge className="bg-yellow-100 text-yellow-800">待下载</Badge>;
      case 'skipped':
        return <Badge className="bg-slate-100 text-slate-800">已存在</Badge>;
      case 'failed':
        return <Badge className="bg-red-100 text-red-800">失败</Badge>;
      default:
        return <Badge variant="secondary">{status}</Badge>;
    }
  };

  return (
    <Tabs defaultValue="topics" className="space-y-4">
      <TabsList className="grid w-full grid-cols-2">
        <TabsTrigger value="topics">话题数据</TabsTrigger>
        <TabsTrigger value="files">文件数据</TabsTrigger>
      </TabsList>

      <TabsContent value="topics">
        <Card className="border border-gray-200 shadow-none">
          <CardHeader>
            <CardTitle>
              {selectedGroup ? `${selectedGroup.name} - 话题列表` : '话题列表'}
            </CardTitle>
            <CardDescription>
              {selectedGroup
                ? `查看 ${selectedGroup.name} 群组的话题数据`
                : '查看已采集的话题数据'
              }
            </CardDescription>
          </CardHeader>
          <CardContent>
            {/* 搜索栏 */}
            <div className="flex gap-2 mb-4">
              <Input
                placeholder="搜索话题标题..."
                value={topicsSearch}
                onChange={(e) => setTopicsSearch(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && handleTopicsSearch()}
              />
              <Button onClick={handleTopicsSearch} disabled={loading}>
                搜索
              </Button>
            </div>

            {/* 话题表格 */}
            {topicsData && (
              <>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>标题</TableHead>
                      <TableHead>创建时间</TableHead>
                      <TableHead>点赞数</TableHead>
                      <TableHead>评论数</TableHead>
                      <TableHead>阅读数</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {topicsData.data.map((topic) => (
                      <TableRow key={topic.topic_id}>
                        <TableCell className="max-w-md">
                          <div className="truncate" title={topic.title}>
                            {topic.title || '无标题'}
                          </div>
                        </TableCell>
                        <TableCell>{formatDate(topic.create_time)}</TableCell>
                        <TableCell>{topic.likes_count}</TableCell>
                        <TableCell>{topic.comments_count}</TableCell>
                        <TableCell>{topic.reading_count}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>

                {/* 分页控制 */}
                <div className="flex items-center justify-between mt-4">
                  <div className="text-sm text-muted-foreground">
                    共 {topicsData.pagination.total} 条记录，第 {topicsData.pagination.page} / {topicsData.pagination.pages} 页
                  </div>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setTopicsPage(Math.max(1, topicsPage - 1))}
                      disabled={topicsPage <= 1 || loading}
                    >
                      上一页
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setTopicsPage(Math.min(topicsData.pagination.pages, topicsPage + 1))}
                      disabled={topicsPage >= topicsData.pagination.pages || loading}
                    >
                      下一页
                    </Button>
                  </div>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </TabsContent>

      <TabsContent value="files">
        <Card className="border border-gray-200 shadow-none">
          <CardHeader>
            <CardTitle>文件列表</CardTitle>
            <CardDescription>查看已收集的文件信息</CardDescription>
          </CardHeader>
          <CardContent>
            {selectedGroupId === undefined && (
              <div className="text-sm text-muted-foreground mb-4">
                请先选择一个群组再查看文件列表
              </div>
            )}
            {/* 状态筛选 */}
            <div className="flex gap-2 mb-4">
              <Select
                value={filesStatus}
                onValueChange={handleFilesStatusChange}
                disabled={selectedGroupId === undefined}
              >
                <SelectTrigger className="w-48">
                  <SelectValue placeholder="选择状态筛选" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="">全部状态</SelectItem>
                  <SelectItem value="pending">待下载</SelectItem>
                  <SelectItem value="completed">已完成</SelectItem>
                  <SelectItem value="downloaded">已完成(旧)</SelectItem>
                  <SelectItem value="skipped">已存在</SelectItem>
                  <SelectItem value="failed">失败</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* 文件表格 */}
            {filesData && (
              <>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>文件名</TableHead>
                      <TableHead>大小</TableHead>
                      <TableHead>下载次数</TableHead>
                      <TableHead>创建时间</TableHead>
                      <TableHead>状态</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filesData.data.map((file) => (
                      <TableRow key={file.file_id}>
                        <TableCell className="max-w-md">
                          <div className="truncate" title={file.name}>
                            {file.name}
                          </div>
                        </TableCell>
                        <TableCell>{formatFileSize(file.size)}</TableCell>
                        <TableCell>{file.download_count}</TableCell>
                        <TableCell>{formatDate(file.create_time)}</TableCell>
                        <TableCell>{getStatusBadge(file.download_status)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>

                {/* 分页控制 */}
                <div className="flex items-center justify-between mt-4">
                  <div className="text-sm text-muted-foreground">
                    共 {filesData.pagination.total} 条记录，第 {filesData.pagination.page} / {filesData.pagination.pages} 页
                  </div>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setFilesPage(Math.max(1, filesPage - 1))}
                      disabled={filesPage <= 1 || loading}
                    >
                      上一页
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setFilesPage(Math.min(filesData.pagination.pages, filesPage + 1))}
                      disabled={filesPage >= filesData.pagination.pages || loading}
                    >
                      下一页
                    </Button>
                  </div>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </TabsContent>
    </Tabs>
  );
}
