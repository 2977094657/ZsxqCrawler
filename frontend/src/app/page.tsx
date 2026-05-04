'use client';

import { useState, useEffect } from 'react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';

import { apiClient, DatabaseStats, Group } from '@/lib/api';
import CrawlPanel from '@/components/CrawlPanel';
import FilePanel from '@/components/FilePanel';
import DataPanel from '@/components/DataPanel';
import TaskPanel from '@/components/TaskPanel';
import ConfigPanel from '@/components/ConfigPanel';
import GroupSelector from '@/components/GroupSelector';
import AccountPanel from '@/components/AccountPanel';
import { toast } from 'sonner';

export default function Home() {
  const [stats, setStats] = useState<DatabaseStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedGroup, setSelectedGroup] = useState<Group | null>(null);

  useEffect(() => {
    loadStats();
  }, []);



  const loadStats = async () => {
    try {
      setLoading(true);
      const data = await apiClient.getDatabaseStats();
      setStats(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : '加载统计信息失败');
    } finally {
      setLoading(false);
    }
  };

  const refreshStats = () => {
    loadStats();
  };



  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <Progress value={undefined} className="w-64 mb-4" />
          <p className="text-muted-foreground">加载中...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <Card className="w-96">
          <CardHeader>
            <CardTitle className="text-red-600">连接错误</CardTitle>
            <CardDescription>无法连接到后端API服务</CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground mb-4">{error}</p>
            <Button onClick={loadStats} className="w-full">
              重试连接
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  // 检查是否已配置
  if (stats && stats.configured === false && !stats.has_local_data) {
    return <ConfigPanel onConfigSaved={loadStats} />;
  }

  // 如果已配置或已有本地导入数据但未选择群组，显示群组选择界面
  if (stats && (stats.configured !== false || stats.has_local_data) && !selectedGroup) {
    return <GroupSelector onGroupSelected={setSelectedGroup} />;
  }

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto p-4">
        {/* 页面标题和群组信息 */}
        <div className="mb-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-muted-foreground">
                知识星球内容爬取与文件下载工具，支持话题采集、评论获取、文件批量下载等功能
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                onClick={() => setSelectedGroup(null)}
                className="flex items-center gap-2"
              >
                ← 返回群组选择
              </Button>
            </div>
          </div>

          {/* 当前选中的群组信息 */}
          {selectedGroup && (
            <Card className="mt-3">
              <CardContent className="py-3">
                <div className="flex items-center gap-4">
                  {selectedGroup.background_url && (
                    <img
                      src={selectedGroup.background_url}
                      alt={selectedGroup.name}
                      className="w-12 h-12 rounded-lg object-cover border border-border"
                    />
                  )}
                  <div className="flex-1">
                    <h2 className="text-lg font-semibold">{selectedGroup.name}</h2>
                    <div className="flex items-center gap-2 mt-1">
                      <Badge variant="secondary">{selectedGroup.type}</Badge>
                      <Badge variant="outline">ID: {selectedGroup.group_id}</Badge>
                      {selectedGroup.owner && (
                        <span className="text-sm text-muted-foreground">
                          群主: {selectedGroup.owner.name}
                        </span>
                      )}
                    </div>
                  </div>
                  {selectedGroup.statistics && (
                    <div className="flex gap-6 text-center">
                      <div>
                        <div className="text-base font-semibold">
                          {selectedGroup.statistics.members?.count || 0}
                        </div>
                        <div className="text-xs text-muted-foreground">成员</div>
                      </div>
                      <div>
                        <div className="text-base font-semibold">
                          {selectedGroup.statistics.topics?.topics_count || 0}
                        </div>
                        <div className="text-xs text-muted-foreground">话题</div>
                      </div>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          )}
        </div>

        {/* 统计概览 */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">话题总数</CardTitle>
              <Badge variant="secondary">📝</Badge>
            </CardHeader>
            <CardContent>
              <div className="text-xl font-semibold">
                {stats?.topic_database.timestamp_info.total_topics || 0}
              </div>
              <p className="text-xs text-muted-foreground">
                {stats?.topic_database.timestamp_info.has_data ? '已有数据' : '暂无数据'}
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">文件总数</CardTitle>
              <Badge variant="secondary">📁</Badge>
            </CardHeader>
            <CardContent>
              <div className="text-xl font-semibold">
                {stats?.file_database.stats.files || 0}
              </div>
              <p className="text-xs text-muted-foreground">
                已收集文件信息
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">数据时间范围</CardTitle>
              <Badge variant="secondary">📅</Badge>
            </CardHeader>
            <CardContent>
              <div className="text-sm">
                {stats?.topic_database.timestamp_info.has_data ? (
                  <>
                    <div className="font-medium">
                      {stats.topic_database.timestamp_info.oldest_timestamp}
                    </div>
                    <div className="text-muted-foreground">至</div>
                    <div className="font-medium">
                      {stats.topic_database.timestamp_info.newest_timestamp}
                    </div>
                  </>
                ) : (
                  <div className="text-muted-foreground">暂无数据</div>
                )}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* 主要功能面板 */}
        <Tabs defaultValue="crawl" className="space-y-3">
          <TabsList className="grid w-full grid-cols-5">
            <TabsTrigger value="crawl">话题采集</TabsTrigger>
            <TabsTrigger value="files">文件管理</TabsTrigger>
            <TabsTrigger value="data">数据查看</TabsTrigger>
            <TabsTrigger value="tasks">任务状态</TabsTrigger>
            <TabsTrigger value="accounts">账号管理</TabsTrigger>
          </TabsList>

          <TabsContent value="crawl">
            <CrawlPanel onStatsUpdate={refreshStats} selectedGroup={selectedGroup} />
          </TabsContent>

          <TabsContent value="files">
            <FilePanel onStatsUpdate={refreshStats} selectedGroup={selectedGroup} />
          </TabsContent>

          <TabsContent value="data">
            <DataPanel selectedGroup={selectedGroup} />
          </TabsContent>

          <TabsContent value="tasks">
            <TaskPanel />
          </TabsContent>

          <TabsContent value="accounts">
            <AccountPanel />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
