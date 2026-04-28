'use client';

import { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog';
import { Settings } from 'lucide-react';
import { apiClient, Group } from '@/lib/api';
import { toast } from 'sonner';
import CrawlSettingsDialog from './CrawlSettingsDialog';
import CrawlLatestDialog from './CrawlLatestDialog';

interface CrawlPanelProps {
  onStatsUpdate: () => void;
  selectedGroup?: Group | null;
}

export default function CrawlPanel({ onStatsUpdate, selectedGroup }: CrawlPanelProps) {
  const [loading, setLoading] = useState<string | null>(null);
  const [historicalPages, setHistoricalPages] = useState(10);
  const [historicalPerPage, setHistoricalPerPage] = useState(20);

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

  const handleCrawlHistorical = async () => {
    if (!selectedGroup) {
      toast.error('请先选择一个群组');
      return;
    }

    try {
      setLoading('historical');
      const response = await apiClient.crawlHistorical(selectedGroup.group_id, historicalPages, historicalPerPage);
      toast.success(`任务已创建: ${response.task_id}`);
      onStatsUpdate();
    } catch (error) {
      toast.error(`创建任务失败: ${error instanceof Error ? error.message : '未知错误'}`);
    } finally {
      setLoading(null);
    }
  };

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

      let response: any;

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
      const response = await apiClient.clearTopicDatabase(selectedGroup.group_id);
      toast.success('话题数据库已清除');
      onStatsUpdate();
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
      <Card>
        <CardHeader>
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
            <p>✅ 默认：直接从最新话题开始增量抓取</p>
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
      {/* 增量爬取历史 */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Badge variant="secondary">📚</Badge>
            增量爬取历史
          </CardTitle>
          <CardDescription>
            基于数据库最老时间戳，精确补充历史数据
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-2">
            <div className="space-y-2">
              <Label htmlFor="historical-pages">爬取页数</Label>
              <Input
                id="historical-pages"
                type="number"
                value={historicalPages}
                onChange={(e) => setHistoricalPages(Number(e.target.value))}
                min={1}
                max={1000}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="historical-per-page">每页数量</Label>
              <Input
                id="historical-per-page"
                type="number"
                value={historicalPerPage}
                onChange={(e) => setHistoricalPerPage(Number(e.target.value))}
                min={1}
                max={100}
              />
            </div>
          </div>
          <Button
            onClick={handleCrawlHistorical}
            disabled={loading === 'historical'}
            className="w-full"
          >
            {loading === 'historical' ? '创建任务中...' : '开始爬取'}
          </Button>
          <div className="text-xs text-muted-foreground">
            <p>✅ 适合：精确补充历史，有目标的回填</p>
            <p>📊 总计爬取: {historicalPages * historicalPerPage} 条记录</p>
          </div>
        </CardContent>
      </Card>

      {/* 获取所有历史数据 */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Badge variant="secondary">🔄</Badge>
            获取所有历史数据
          </CardTitle>
          <CardDescription>
            无限爬取，从最老数据无限挖掘
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="text-sm text-muted-foreground space-y-2">
            <p>⚠️ 这是一个长时间运行的任务</p>
            <p>🔄 将持续爬取直到没有更多历史数据</p>
            <p>📈 适合：全量归档，完整数据收集</p>
          </div>
          
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button
                variant="destructive"
                disabled={loading === 'all'}
                className="w-full"
              >
                {loading === 'all' ? '创建任务中...' : '开始全量爬取'}
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>确认全量爬取</AlertDialogTitle>
                <AlertDialogDescription>
                  这将开始一个长时间运行的任务，持续爬取所有历史数据直到完成。
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
