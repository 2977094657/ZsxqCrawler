'use client';

import { useState, useEffect, useCallback } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog';
import { apiClient, Group } from '@/lib/api';
import { toast } from 'sonner';

interface FilePanelProps {
  onStatsUpdate: () => void;
  selectedGroup?: Group | null;
}

interface FileStats {
  database_stats: Record<string, number>;
  download_stats: {
    total_files: number;
    downloaded: number;
    pending: number;
    failed: number;
  };
}

export default function FilePanel({ onStatsUpdate, selectedGroup }: FilePanelProps) {
  const [loading, setLoading] = useState<string | null>(null);
  const [fileStats, setFileStats] = useState<FileStats | null>(null);
  const [maxFiles, setMaxFiles] = useState<number | undefined>(undefined);
  const [sortBy, setSortBy] = useState('download_count');

  const loadFileStats = useCallback(async () => {
    if (!selectedGroup) {
      setFileStats(null);
      return;
    }

    try {
      const stats = await apiClient.getFileStats(selectedGroup.group_id);
      setFileStats(stats);
    } catch (error) {
      console.error('加载文件统计失败:', error);
    }
  }, [selectedGroup]);

  useEffect(() => {
    loadFileStats();
  }, [loadFileStats]);

  const handleClearFileDatabase = async () => {
    if (!selectedGroup) {
      toast.error('请先选择一个群组');
      return;
    }

    try {
      setLoading('clear');
      await apiClient.clearFileDatabase(selectedGroup.group_id);
      toast.success('文件数据库已清除');
      onStatsUpdate();
      loadFileStats();
    } catch (error) {
      toast.error(`清除数据库失败: ${error instanceof Error ? error.message : '未知错误'}`);
    } finally {
      setLoading(null);
    }
  };

  const handleDownloadFiles = async () => {
    if (!selectedGroup) {
      toast.error('请先选择一个群组');
      return;
    }

    try {
      setLoading('download');
      const response = await apiClient.downloadFiles(selectedGroup.group_id, maxFiles, sortBy);
      toast.success(`任务已创建: ${response.task_id}`);
      onStatsUpdate();
      loadFileStats();
    } catch (error) {
      toast.error(`创建任务失败: ${error instanceof Error ? error.message : '未知错误'}`);
    } finally {
      setLoading(null);
    }
  };

  return (
    <div className="space-y-4">
      {!selectedGroup && (
        <div className="text-sm text-muted-foreground">
          请先选择一个群组再进行文件收集与下载
        </div>
      )}
      {/* 文件统计概览 */}
      {fileStats && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">文件总数</CardTitle>
              <Badge variant="secondary">📁</Badge>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{fileStats.download_stats.total_files}</div>
              <p className="text-xs text-muted-foreground">已收集文件信息</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">已下载</CardTitle>
              <Badge variant="secondary" className="bg-green-100 text-green-800">✅</Badge>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-600">{fileStats.download_stats.downloaded}</div>
              <p className="text-xs text-muted-foreground">下载完成</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">待下载</CardTitle>
              <Badge variant="secondary" className="bg-yellow-100 text-yellow-800">⏳</Badge>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-yellow-600">{fileStats.download_stats.pending}</div>
              <p className="text-xs text-muted-foreground">等待下载</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">下载失败</CardTitle>
              <Badge variant="secondary" className="bg-red-100 text-red-800">❌</Badge>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-red-600">{fileStats.download_stats.failed}</div>
              <p className="text-xs text-muted-foreground">需要重试</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* 功能操作面板 */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* 下载文件 */}
        <Card className="border border-gray-200 shadow-none">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Badge variant="secondary">⬇️</Badge>
              下载文件
            </CardTitle>
            <CardDescription>
              自动收集文件列表并根据设置的条件批量下载
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="max-files">最大下载文件数</Label>
              <Input
                id="max-files"
                type="number"
                placeholder="留空表示无限制"
                value={maxFiles || ''}
                onChange={(e) => setMaxFiles(e.target.value ? Number(e.target.value) : undefined)}
                min={1}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="sort-by">排序方式</Label>
              <Select value={sortBy} onValueChange={setSortBy}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="download_count">按下载次数排序</SelectItem>
                  <SelectItem value="time">按时间顺序排序</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <Button
              onClick={handleDownloadFiles}
              disabled={loading === 'download' || !selectedGroup}
              className="w-full"
            >
              {loading === 'download' ? '创建任务中...' : '开始下载'}
            </Button>

            <div className="text-xs text-muted-foreground space-y-1">
              <p>🔍 自动收集文件列表</p>
              <p>📁 文件将保存到 downloads 目录</p>
              <p>🔄 支持断点续传和重复检测</p>
            </div>
          </CardContent>
        </Card>

        {/* 清除下载数据库 */}
        <Card className="border border-gray-200 shadow-none">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Badge variant="destructive">🗑️</Badge>
              清除下载数据库
            </CardTitle>
            <CardDescription>
              清除所有文件记录和下载状态
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="text-sm text-muted-foreground space-y-2">
              <p>⚠️ 将删除所有文件记录</p>
              <p>🔄 清除下载状态和进度</p>
              <p>💾 不会删除已下载的文件</p>
            </div>
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button
                  disabled={loading === 'clear' || !selectedGroup}
                  variant="destructive"
                  className="w-full"
                >
                  {loading === 'clear' ? '清除中...' : '清除数据库'}
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle className="text-red-600">确认清除文件数据库</AlertDialogTitle>
                  <AlertDialogDescription className="text-red-700">
                    这将永久删除所有文件记录和下载状态，但不会删除已下载的文件。
                    此操作不可恢复，确定要继续吗？
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>取消</AlertDialogCancel>
                  <AlertDialogAction
                    onClick={handleClearFileDatabase}
                    className="bg-red-600 hover:bg-red-700 focus:ring-red-600"
                  >
                    确认清除
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
