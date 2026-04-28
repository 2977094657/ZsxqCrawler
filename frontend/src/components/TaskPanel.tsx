'use client';

import { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { apiClient, Task } from '@/lib/api';

export default function TaskPanel() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);

  useEffect(() => {
    loadTasks();
  }, []);

  useEffect(() => {
    if (!autoRefresh) return;

    const interval = setInterval(() => {
      loadTasks();
    }, 3000); // 每3秒刷新一次

    return () => clearInterval(interval);
  }, [autoRefresh]);

  const loadTasks = async () => {
    try {
      setLoading(true);
      const data = await apiClient.getTasks();
      setTasks(data.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()));
    } catch (error) {
      console.error('加载任务列表失败:', error);
    } finally {
      setLoading(false);
    }
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'pending':
        return <Badge variant="secondary">⏳ 等待中</Badge>;
      case 'running':
        return <Badge className="bg-blue-100 text-blue-800">🔄 运行中</Badge>;
      case 'completed':
        return <Badge className="bg-green-100 text-green-800">✅ 已完成</Badge>;
      case 'failed':
        return <Badge className="bg-red-100 text-red-800">❌ 失败</Badge>;
      default:
        return <Badge variant="secondary">{status}</Badge>;
    }
  };

  const getTaskTypeLabel = (type: string) => {
    switch (type) {
      case 'crawl_latest':
        return '🆕 获取最新记录';
      case 'crawl_historical':
        return '📚 增量爬取历史';
      case 'crawl_all':
        return '🔄 全量爬取';
      case 'collect_files':
        return '📋 收集文件列表';
      case 'download_files':
        return '⬇️ 下载文件';
      case 'crawl_time_range':
        return '🗓️ 按时间区间爬取';
      default:
        return type;
    }
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString('zh-CN');
  };

  const formatDuration = (startTime: string, endTime?: string) => {
    const start = new Date(startTime).getTime();
    const end = endTime ? new Date(endTime).getTime() : Date.now();
    const duration = Math.floor((end - start) / 1000);
    
    if (duration < 60) {
      return `${duration}秒`;
    } else if (duration < 3600) {
      return `${Math.floor(duration / 60)}分${duration % 60}秒`;
    } else {
      const hours = Math.floor(duration / 3600);
      const minutes = Math.floor((duration % 3600) / 60);
      return `${hours}小时${minutes}分`;
    }
  };

  const getRunningTasks = () => tasks.filter(task => task.status === 'running');
  const getCompletedTasks = () => tasks.filter(task => task.status === 'completed');
  const getFailedTasks = () => tasks.filter(task => task.status === 'failed');

  return (
    <div className="space-y-4">
      {/* 任务统计概览 */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">总任务数</CardTitle>
            <Badge variant="secondary">📊</Badge>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{tasks.length}</div>
            <p className="text-xs text-muted-foreground">所有任务</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">运行中</CardTitle>
            <Badge variant="secondary" className="bg-blue-100 text-blue-800">🔄</Badge>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-blue-600">{getRunningTasks().length}</div>
            <p className="text-xs text-muted-foreground">正在执行</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">已完成</CardTitle>
            <Badge variant="secondary" className="bg-green-100 text-green-800">✅</Badge>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-green-600">{getCompletedTasks().length}</div>
            <p className="text-xs text-muted-foreground">执行成功</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">失败</CardTitle>
            <Badge variant="secondary" className="bg-red-100 text-red-800">❌</Badge>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-red-600">{getFailedTasks().length}</div>
            <p className="text-xs text-muted-foreground">需要处理</p>
          </CardContent>
        </Card>
      </div>

      {/* 任务列表 */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>任务列表</CardTitle>
              <CardDescription>查看所有任务的执行状态和结果</CardDescription>
            </div>
            <div className="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => setAutoRefresh(!autoRefresh)}
              >
                {autoRefresh ? '🔄 自动刷新' : '⏸️ 手动刷新'}
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={loadTasks}
                disabled={loading}
              >
                {loading ? '刷新中...' : '立即刷新'}
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {tasks.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              暂无任务记录
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>任务类型</TableHead>
                  <TableHead>状态</TableHead>
                  <TableHead>消息</TableHead>
                  <TableHead>创建时间</TableHead>
                  <TableHead>耗时</TableHead>
                  <TableHead>结果</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {tasks.map((task) => (
                  <TableRow key={task.task_id}>
                    <TableCell>
                      <div className="font-medium">
                        {getTaskTypeLabel(task.type)}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        {task.task_id}
                      </div>
                    </TableCell>
                    <TableCell>{getStatusBadge(task.status)}</TableCell>
                    <TableCell className="max-w-md">
                      <div className="truncate" title={task.message}>
                        {task.message}
                      </div>
                    </TableCell>
                    <TableCell>{formatDate(task.created_at)}</TableCell>
                    <TableCell>
                      {formatDuration(task.created_at, task.status === 'running' ? undefined : task.updated_at)}
                    </TableCell>
                    <TableCell>
                      {task.result ? (
                        <div className="text-xs">
                          {task.result.new_topics && (
                            <div>新增: {task.result.new_topics}</div>
                          )}
                          {task.result.updated_topics && (
                            <div>更新: {task.result.updated_topics}</div>
                          )}
                          {task.result.downloaded_files && (
                            <div>下载: {task.result.downloaded_files}</div>
                          )}
                        </div>
                      ) : (
                        <span className="text-muted-foreground">-</span>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* 运行中任务的进度显示 */}
      {getRunningTasks().length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>运行中的任务</CardTitle>
            <CardDescription>正在执行的任务详情</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {getRunningTasks().map((task) => (
              <div key={task.task_id} className="space-y-2">
                <div className="flex items-center justify-between">
                  <span className="font-medium">{getTaskTypeLabel(task.type)}</span>
                  <Badge className="bg-blue-100 text-blue-800">运行中</Badge>
                </div>
                <Progress value={undefined} className="w-full" />
                <p className="text-sm text-muted-foreground">{task.message}</p>
              </div>
            ))}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
