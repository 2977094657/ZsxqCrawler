'use client';

import { useEffect, useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog';
import { Settings } from 'lucide-react';
import { apiClient, Group, GroupStats, TaskCreateResponse } from '@/lib/api';
import { toast } from 'sonner';
import CrawlSettingsDialog from './CrawlSettingsDialog';
import CrawlLatestDialog from './CrawlLatestDialog';
import ModeTip from './ModeTip';

interface CrawlPanelProps {
  onStatsUpdate: () => void;
  selectedGroup?: Group | null;
}

export default function CrawlPanel({ onStatsUpdate, selectedGroup }: CrawlPanelProps) {
  const [loading, setLoading] = useState<string | null>(null);
  const [localGroupStats, setLocalGroupStats] = useState<GroupStats | null>(null);

  // 添加组件实例标识
  const instanceId = Math.random().toString(36).substr(2, 9);
  console.log(`🏷️ CrawlPanel实例 ${instanceId} 已创建`);

  // 爬取设置状态
  const [crawlSettingsOpen, setCrawlSettingsOpen] = useState(false);
  const [crawlLatestOpen, setCrawlLatestOpen] = useState(false);
  const [crawlInterval, setCrawlInterval] = useState(3.5);
  const [longSleepInterval, setLongSleepInterval] = useState(240);
  const [pagesPerBatch, setPagesPerBatch] = useState(15);
  const [crawlIntervalMin, setCrawlIntervalMin] = useState<number>(2);
  const [crawlIntervalMax, setCrawlIntervalMax] = useState<number>(5);
  const [longSleepIntervalMin, setLongSleepIntervalMin] = useState<number>(180);
  const [longSleepIntervalMax, setLongSleepIntervalMax] = useState<number>(300);

  const refreshLocalGroupStats = async () => {
    if (!selectedGroup) {
      setLocalGroupStats(null);
      return;
    }
    try {
      const stats = await apiClient.getGroupStats(selectedGroup.group_id);
      setLocalGroupStats(stats);
    } catch (error) {
      console.warn('加载群组本地统计失败:', error);
      setLocalGroupStats(null);
    }
  };

  useEffect(() => {
    let cancelled = false;

    if (!selectedGroup) {
      setLocalGroupStats(null);
      return;
    }

    apiClient.getGroupStats(selectedGroup.group_id)
      .then((stats) => {
        if (!cancelled) {
          setLocalGroupStats(stats);
        }
      })
      .catch((error) => {
        console.warn('加载群组本地统计失败:', error);
        if (!cancelled) {
          setLocalGroupStats(null);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [selectedGroup]);

  const hasLocalTopics = (localGroupStats?.topics_count || 0) > 0;
  const allModeTitle = hasLocalTopics ? '继续爬取历史' : '获取所有历史数据';
  const allModeButtonText = hasLocalTopics ? '继续爬取' : '全量爬取';
  const allModeConfirmTitle = hasLocalTopics ? '确认继续爬取' : '确认全量爬取';
  const allModeConfirmText = hasLocalTopics
    ? '当前群组已有本地数据，将从数据库中最老话题时间继续向更早历史爬取，直到没有更多数据。'
    : '当前群组暂无本地话题数据，将从最新话题开始持续向历史爬取，直到没有更多数据。';

  const handleCrawlAll = async () => {
    if (!selectedGroup) {
      toast.error('请先选择一个群组');
      return;
    }

    try {
      setLoading('all');

      // 构建爬取设置
      console.log(`🚀 CrawlPanel实例 ${instanceId} 构建爬取设置前的状态值:`);
      console.log('  crawlIntervalMin:', crawlIntervalMin);
      console.log('  crawlIntervalMax:', crawlIntervalMax);
      console.log('  longSleepIntervalMin:', longSleepIntervalMin);
      console.log('  longSleepIntervalMax:', longSleepIntervalMax);
      console.log('  pagesPerBatch:', pagesPerBatch);

      const crawlSettings = {
        crawlIntervalMin,
        crawlIntervalMax,
        longSleepIntervalMin,
        longSleepIntervalMax,
        pagesPerBatch: Math.max(pagesPerBatch, 5)
      };

      console.log(`🚀 CrawlPanel实例 ${instanceId} 最终发送的爬取设置:`, crawlSettings);

      const response = await apiClient.crawlAll(selectedGroup.group_id, crawlSettings);
      toast.success(`任务已创建: ${response.task_id}`);
      onStatsUpdate();
      setTimeout(() => {
        refreshLocalGroupStats();
      }, 2000);
    } catch (error) {
      toast.error(`创建任务失败: ${error instanceof Error ? error.message : '未知错误'}`);
    } finally {
      setLoading(null);
    }
  };
  
  const handleCrawlLatestConfirm = async (params: {
    mode: 'latest' | 'range';
    startTime?: string;
    endTime?: string;
    lastDays?: number;
    perPage?: number;
  }) => {
    if (!selectedGroup) {
      toast.error('请先选择一个群组');
      return;
    }

    try {
      setLoading('latest');

      // 构建爬取设置
      const crawlSettings = {
        crawlIntervalMin,
        crawlIntervalMax,
        longSleepIntervalMin,
        longSleepIntervalMax,
        pagesPerBatch: Math.max(pagesPerBatch, 5),
      };

      let response: TaskCreateResponse;

      if (params.mode === 'latest') {
        response = await apiClient.crawlLatestUntilComplete(selectedGroup.group_id, crawlSettings);
      } else {
        response = await apiClient.crawlByTimeRange(selectedGroup.group_id, {
          startTime: params.startTime,
          endTime: params.endTime,
          lastDays: params.lastDays,
          perPage: params.perPage,
          crawlIntervalMin,
          crawlIntervalMax,
          longSleepIntervalMin,
          longSleepIntervalMax,
          pagesPerBatch: Math.max(pagesPerBatch, 5),
        });
      }

      toast.success(`任务已创建: ${response.task_id}`);
      onStatsUpdate();
      setCrawlLatestOpen(false);
    } catch (error) {
      toast.error(`创建任务失败: ${error instanceof Error ? error.message : '未知错误'}`);
    } finally {
      setLoading(null);
    }
  };
  
  // 处理爬取设置变更
  const handleCrawlSettingsChange = (settings: {
    crawlInterval: number;
    longSleepInterval: number;
    pagesPerBatch: number;
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
  }) => {
    console.log(`🔧 CrawlPanel实例 ${instanceId} 收到爬取设置变更:`, settings);
    setCrawlInterval(settings.crawlInterval);
    setLongSleepInterval(settings.longSleepInterval);
    setPagesPerBatch(settings.pagesPerBatch);
    setCrawlIntervalMin(settings.crawlIntervalMin || 2);
    setCrawlIntervalMax(settings.crawlIntervalMax || 5);
    setLongSleepIntervalMin(settings.longSleepIntervalMin || 180);
    setLongSleepIntervalMax(settings.longSleepIntervalMax || 300);

    console.log('🔧 设置后的状态值:');
    console.log('  crawlIntervalMin:', settings.crawlIntervalMin || 2);
    console.log('  crawlIntervalMax:', settings.crawlIntervalMax || 5);
    console.log('  longSleepIntervalMin:', settings.longSleepIntervalMin || 180);
    console.log('  longSleepIntervalMax:', settings.longSleepIntervalMax || 300);
    console.log('  pagesPerBatch:', settings.pagesPerBatch);
  };

  const handleClearTopicDatabase = async () => {
    if (!selectedGroup) {
      toast.error('请先选择一个群组');
      return;
    }

    try {
      setLoading('clear');
      await apiClient.clearTopicDatabase(selectedGroup.group_id);
      toast.success('话题数据库已清除');
      onStatsUpdate();
      await refreshLocalGroupStats();
    } catch (error) {
      toast.error(`清除数据库失败: ${error instanceof Error ? error.message : '未知错误'}`);
    } finally {
      setLoading(null);
    }
  };

  return (
    <div className="space-y-4">
      {/* 爬取设置 */}
      <div className="flex justify-between items-center">
        <div>
          <h3 className="text-lg font-semibold">话题采集</h3>
          <p className="text-sm text-muted-foreground">配置爬取间隔和批次设置</p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => setCrawlSettingsOpen(true)}
          className="flex items-center gap-2"
        >
          <Settings className="h-4 w-4" />
          爬取设置
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {/* 获取最新话题 */}
      <Card className="relative">
        <ModeTip>
          <div className="space-y-1">
            <p className="font-medium text-gray-900">获取最新话题</p>
            <p>从最新话题开始抓取；如果本地已有数据，会向后抓到与本地数据衔接为止。</p>
            <p>也可以在弹窗中选择最近 N 天或自定义时间范围。</p>
          </div>
        </ModeTip>
        <CardHeader className="pr-10">
          <CardTitle className="flex items-center gap-2">
            <Badge variant="secondary">🆕</Badge>
            获取最新话题
          </CardTitle>
          <CardDescription>
            默认从最新开始，也可按时间区间采集
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="text-sm text-muted-foreground space-y-2">
            <p>✅ 默认：直接从最新话题开始同步新内容</p>
            <p>🕒 可选：按时间区间采集（首次也可用）</p>
          </div>
          <Button
            onClick={() => setCrawlLatestOpen(true)}
            disabled={loading === 'latest'}
            className="w-full"
          >
            {loading === 'latest' ? '创建任务中...' : '获取最新'}
          </Button>
        </CardContent>
      </Card>

      {/* 获取所有历史数据 */}
      <Card className="relative">
        <ModeTip>
          <div className="space-y-1">
            <p className="font-medium text-gray-900">{allModeTitle}</p>
            <p>无本地数据时：从最新话题开始做完整归档。</p>
            <p>已有本地数据时：从数据库最老话题时间继续向历史爬，不会清空重爬。</p>
            <p>任务会一直运行到没有更多历史数据。</p>
          </div>
        </ModeTip>
        <CardHeader className="pr-10">
          <CardTitle className="flex items-center gap-2">
            <Badge variant="secondary">🔄</Badge>
            {allModeTitle}
            {!hasLocalTopics && (
              <Badge variant="secondary" className="text-xs">推荐</Badge>
            )}
          </CardTitle>
          <CardDescription>
            {hasLocalTopics ? '从本地最老话题继续向历史爬取' : '首次采集推荐使用，完整收集历史数据'}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="text-sm text-muted-foreground space-y-2">
            <p>⚠️ 这是一个长时间运行的任务</p>
            <p>{hasLocalTopics ? '🔄 已有本地数据，将从最老话题继续向前爬' : '📈 暂无本地数据，推荐用此模式做首次全量归档'}</p>
            <p>✅ 将持续爬取直到没有更多历史数据</p>
          </div>
          
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button
                variant="destructive"
                disabled={loading === 'all'}
                className="w-full"
              >
                {loading === 'all' ? '创建任务中...' : allModeButtonText}
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>{allModeConfirmTitle}</AlertDialogTitle>
                <AlertDialogDescription>
                  {allModeConfirmText}
                  <br />
                  任务可能需要数小时甚至更长时间，确定要继续吗？
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>取消</AlertDialogCancel>
                <AlertDialogAction onClick={handleCrawlAll}>
                  确认开始
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </CardContent>
      </Card>

      {/* 清除话题数据库 */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Badge variant="destructive">🗑️</Badge>
            清除话题数据库
          </CardTitle>
          <CardDescription>
            清除所有话题、评论、用户等数据
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="text-sm text-muted-foreground space-y-2">
            <p>⚠️ 将删除所有话题数据</p>
            <p>🔄 清除评论、用户、图片等</p>
            <p>💾 不会删除配置和设置</p>
          </div>

          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button
                variant="destructive"
                disabled={loading === 'clear'}
                className="w-full"
              >
                {loading === 'clear' ? '清除中...' : '清除数据库'}
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle className="text-red-600">确认清除数据库</AlertDialogTitle>
                <AlertDialogDescription className="text-red-700">
                  这将永久删除所有话题数据，包括话题、评论、用户信息等。
                  此操作不可恢复，确定要继续吗？
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>取消</AlertDialogCancel>
                <AlertDialogAction
                  onClick={handleClearTopicDatabase}
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

      <CrawlLatestDialog
        open={crawlLatestOpen}
        onOpenChange={setCrawlLatestOpen}
        submitting={loading === 'latest'}
        onConfirm={handleCrawlLatestConfirm}
        defaultLastDays={7}
        defaultPerPage={20}
      />
      {/* 爬取设置对话框 */}
      <CrawlSettingsDialog
        open={crawlSettingsOpen}
        onOpenChange={setCrawlSettingsOpen}
        crawlInterval={crawlInterval}
        longSleepInterval={longSleepInterval}
        pagesPerBatch={pagesPerBatch}
        onSettingsChange={handleCrawlSettingsChange}
      />
    </div>
  );
}
