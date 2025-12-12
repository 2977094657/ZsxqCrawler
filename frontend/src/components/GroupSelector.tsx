'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Users, MessageSquare, Crown, UserCog, RefreshCw, Trash2 } from 'lucide-react';
import { apiClient, Group, GroupStats, AccountSelf } from '@/lib/api';
import { toast } from 'sonner';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog';
import SafeImage from './SafeImage';
import '../styles/group-selector.css';

interface GroupSelectorProps {
  onGroupSelected: (group: Group) => void;
}

export default function GroupSelector({ onGroupSelected }: GroupSelectorProps) {
  const router = useRouter();
  const [groups, setGroups] = useState<Group[]>([]);
  const [groupStats, setGroupStats] = useState<Record<number, GroupStats>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retryCount, setRetryCount] = useState(0);
  const [isRetrying, setIsRetrying] = useState(false);
  const [accountSelfMap, setAccountSelfMap] = useState<Record<number, AccountSelf | null>>({});
  const [deletingGroups, setDeletingGroups] = useState<Set<number>>(new Set());

  useEffect(() => {
    loadGroups();
  }, []);

  // 监听页面可见性变化和窗口焦点，返回页面时自动刷新群组列表
  // 使用节流避免频繁刷新
  useEffect(() => {
    let lastRefresh = 0;
    const REFRESH_INTERVAL = 5000; // 最少间隔 5 秒

    const maybeRefresh = () => {
      const now = Date.now();
      if (now - lastRefresh > REFRESH_INTERVAL) {
        lastRefresh = now;
        loadGroups(0);
      }
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        maybeRefresh();
      }
    };
    const handleFocus = () => {
      maybeRefresh();
    };
    document.addEventListener('visibilitychange', handleVisibilityChange);
    window.addEventListener('focus', handleFocus);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange);
      window.removeEventListener('focus', handleFocus);
    };
  }, []);

  const loadGroups = async (currentRetryCount = 0) => {
    try {
      if (currentRetryCount === 0) {
        setLoading(true);
        setError(null);
        setRetryCount(0);
        setIsRetrying(false);
      } else {
        setIsRetrying(true);
        setRetryCount(currentRetryCount);
      }

      const data = await apiClient.getGroups();

      // 检查返回数据（允许为空，显示空态，不再抛错）

      setGroups(data.groups);

      // 并发拉取每个群组的所属账号用户信息（头像/昵称等）
      try {
        const selfPromises = data.groups.map(async (group: Group) => {
          try {
            const res = await apiClient.getGroupAccountSelf(group.group_id);
            return { groupId: group.group_id, self: (res as any)?.self || null };
          } catch {
            return { groupId: group.group_id, self: null };
          }
        });
        const selfResults = await Promise.all(selfPromises);
        const selfMap: Record<number, AccountSelf | null> = {};
        selfResults.forEach(({ groupId, self }) => {
          selfMap[groupId] = self;
        });
        setAccountSelfMap(selfMap);
      } catch (e) {
        // 忽略单独失败
        console.warn('加载群组账号用户信息失败:', e);
      }

      // 加载每个群组的统计信息
      const statsPromises = data.groups.map(async (group: Group) => {
        try {
          const stats = await apiClient.getGroupStats(group.group_id);
          return { groupId: group.group_id, stats };
        } catch (error) {
          console.warn(`获取群组 ${group.group_id} 统计信息失败:`, error);
          return { groupId: group.group_id, stats: null };
        }
      });

      const statsResults = await Promise.all(statsPromises);
      const statsMap: Record<number, GroupStats> = {};
      statsResults.forEach(({ groupId, stats }) => {
        if (stats) {
          statsMap[groupId] = stats;
        }
      });
      setGroupStats(statsMap);

      // 成功获取数据，重置状态
      setError(null);
      setRetryCount(0);
      setIsRetrying(false);
      setLoading(false);

    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : '加载群组列表失败';

      // 如果是API保护机制导致的错误，持续重试
      if (errorMessage.includes('未知错误') || errorMessage.includes('空数据') || errorMessage.includes('反爬虫')) {
        const nextRetryCount = currentRetryCount + 1;
        const delay = Math.min(1000 + (nextRetryCount * 500), 5000); // 递增延迟，最大5秒

        console.log(`群组列表加载失败，正在重试 (第${nextRetryCount}次)...`);

        setTimeout(() => {
          loadGroups(nextRetryCount);
        }, delay);
        return;
      }

      // 其他错误，停止重试
      setError(errorMessage);
      setIsRetrying(false);
      setLoading(false);
    }
  };



  const handleRefresh = async () => {
    try {
      await apiClient.refreshLocalGroups();
      await loadGroups(0);
      toast.success('已刷新本地群目录');
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      toast.error(`刷新失败: ${msg}`);
    }
  };

  const handleDeleteGroup = async (groupId: number) => {
    if (deletingGroups.has(groupId)) return;
    setDeletingGroups((prev) => new Set(prev).add(groupId));
    try {
      const res = await apiClient.deleteGroup(groupId);
      const msg = (res as any)?.message || '已删除';
      toast.success(msg);
      await loadGroups(0);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      toast.error(`删除失败: ${msg}`);
    } finally {
      setDeletingGroups((prev) => {
        const s = new Set(prev);
        s.delete(groupId);
        return s;
      });
    }
  };

  const formatDate = (dateString?: string) => {
    if (!dateString) return '';
    try {
      return new Date(dateString).toLocaleDateString('zh-CN');
    } catch {
      return '';
    }
  };

  const getGradientByType = (type: string) => {
    switch (type) {
      case 'private':
        return 'from-purple-400 to-pink-500';
      case 'public':
        return 'from-blue-400 to-cyan-500';
      default:
        return 'from-gray-400 to-gray-600';
    }
  };

  // 判断是否即将过期（过期前一个月）
  const isExpiringWithinMonth = (expiryTime?: string) => {
    if (!expiryTime) return false;
    const expiryDate = new Date(expiryTime);
    const now = new Date();
    const oneMonthFromNow = new Date();
    oneMonthFromNow.setMonth(now.getMonth() + 1);

    return expiryDate <= oneMonthFromNow && expiryDate > now;
  };

  if (loading || isRetrying) {
    return (
      <div className="min-h-screen bg-background">
        <div className="container mx-auto p-4">
          <div className="mb-4">
            <h1 className="text-2xl font-bold mb-1">🌟 知识星球数据采集器</h1>
            <p className="text-sm text-muted-foreground">
              {isRetrying ? '正在重试获取群组列表...' : '正在加载您的知识星球群组...'}
            </p>
          </div>
          <div className="flex items-center justify-center py-8">
            <div className="text-center">
              <Progress value={undefined} className="w-64 mb-4" />
              <p className="text-muted-foreground">
                {isRetrying ? `正在重试... (第${retryCount}次)` : '加载群组列表中...'}
              </p>
              {isRetrying && (
                <p className="text-xs text-gray-400 mt-2">
                  检测到API防护机制，正在自动重试获取数据
                </p>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-background">
        <div className="container mx-auto p-4">
          <div className="mb-4">
            <h1 className="text-2xl font-bold mb-1">🌟 知识星球数据采集器</h1>
            <p className="text-sm text-muted-foreground">
              加载群组列表时出现错误
            </p>
          </div>
          <Card className="max-w-md mx-auto">
            <CardHeader>
              <CardTitle className="text-red-600">加载失败</CardTitle>
              <CardDescription>无法获取群组列表</CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground mb-4">{error}</p>
              <Button onClick={loadGroups} className="w-full">
                重试
              </Button>
            </CardContent>
          </Card>
        </div>
      </div>
    );
  }

  // 按来源拆分群组：网络群组（账号）与本地群组
  // 说明：凡是包含 account 的都视为“网络群组”；凡是包含 local 的都视为“本地群组”
  // 这样 account|local 这类“既有账号又有本地数据”的群，会在两个 Tab 都展示，
  // 满足你在网络和本地视角下都能看到完整信息的需求。
  const accountGroups = groups.filter((g) => !g.source || g.source.includes('account'));
  const localGroups = groups.filter((g) => g.source && g.source.includes('local'));

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto p-4">
        <div className="mb-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold mb-1">🌟 知识星球数据采集器</h1>
              <p className="text-sm text-muted-foreground">
                选择要操作的知识星球群组
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                onClick={handleRefresh}
                className="flex items-center gap-2"
              >
                <RefreshCw className="h-4 w-4" />
                刷新本地群
              </Button>
              <Button
                variant="outline"
                onClick={() => router.push('/accounts')}
                className="flex items-center gap-2"
              >
                <UserCog className="h-4 w-4" />
                账号管理
              </Button>
            </div>
          </div>
        </div>

        {/* 群组统计 */}
        <div className="mb-4 space-y-0.5">
          <p className="text-sm text-muted-foreground">
            共 {accountGroups.length} 个网络群组，{localGroups.length} 个本地群组
          </p>
        </div>

        {/* 群组网格：通过标签区分账号群组与本地群组，禁止混在同一列表中 */}
        <Tabs defaultValue="account" className="space-y-3">
          <TabsList className="grid w-full grid-cols-2 h-9 text-sm">
            <TabsTrigger value="account">网络群组（账号）</TabsTrigger>
            <TabsTrigger value="local">本地群组</TabsTrigger>
          </TabsList>

          {/* 网络群组 */}
          <TabsContent value="account">
            {accountGroups.length === 0 ? (
              <Card className="max-w-md mx-auto border border-gray-200 shadow-none">
                <CardContent className="pt-6">
                  <div className="text-center">
                    <p className="text-muted-foreground">
                      暂无可访问的网络群组，请先在账号管理中添加或更新 Cookie
                    </p>
                  </div>
                </CardContent>
              </Card>
            ) : (
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 xl:grid-cols-6 gap-4">
                {accountGroups.map((group) => {
              const stats = groupStats[group.group_id];
              return (
                <div
                  key={group.group_id}
                  className="group-card cursor-pointer bg-white border border-gray-200 rounded-lg hover:border-gray-300 transition-all duration-200 hover:shadow-md overflow-hidden w-[200px]"
                  onClick={() => router.push(`/groups/${group.group_id}`)}
                >
                  {/* 群组封面：固定200x200 */}
                  <div className="w-[200px] h-[200px]">
                    <SafeImage
                      src={group.background_url}
                      alt={group.name}
                      className="w-full h-full object-cover"
                      fallbackClassName="w-full h-full bg-gradient-to-br"
                      fallbackText={group.name.slice(0, 2)}
                      fallbackGradient={getGradientByType(group.type)}
                    />
                  </div>

                  {/* 内容区域 */}
                  <div className="p-2.5">
                    {/* 群组名称 */}
                    <h3 className="text-sm font-semibold text-gray-900 line-clamp-1 mb-1.5">
                      {group.name}
                    </h3>

                    {/* 统计信息 */}
                    <div className="flex items-center justify-between text-xs text-gray-500 mb-1.5">
                      {/* 群主信息 */}
                      {group.owner && (
                        <div className="flex items-center gap-1">
                          <Crown className="h-3 w-3" />
                          <span className="truncate max-w-[60px]">{group.owner.name}</span>
                        </div>
                      )}

                      {/* 话题数量 */}
                      {stats && (
                        <div className="flex items-center gap-1">
                          <MessageSquare className="h-3 w-3" />
                          <span>{stats.topics_count || 0}</span>
                        </div>
                      )}
                    </div>

                    {/* 类型标识和删除 */}
                    <div className="flex items-center justify-between">
                      {/* 根据付费状态显示不同颜色 */}
                      {group.type === 'pay' ? (
                        group.status === 'expired' ? (
                          <Badge variant="destructive" className="text-xs px-1.5 py-0 h-5">
                            已过期
                          </Badge>
                        ) : isExpiringWithinMonth(group.expiry_time) ? (
                          <Badge variant="outline" className="text-xs px-1.5 py-0 h-5 text-yellow-600 border-yellow-200">
                            即将过期
                          </Badge>
                        ) : (
                          <Badge className={`text-xs px-1.5 py-0 h-5 ${group.is_trial ? 'bg-purple-600' : 'bg-green-600'}`}>
                            {group.is_trial ? '试用' : '付费'}
                          </Badge>
                        )
                      ) : (
                        <Badge variant="secondary" className="text-xs px-1.5 py-0 h-5">
                          免费
                        </Badge>
                      )}

                      {/* 删除按钮 */}
                      <AlertDialog>
                        <AlertDialogTrigger asChild>
                          <button
                            type="button"
                            onClick={(e) => { e.stopPropagation(); }}
                            className="p-1 text-gray-400 hover:text-red-600 transition-colors"
                            title="删除本地数据"
                            disabled={deletingGroups.has(group.group_id)}
                          >
                            <Trash2 className="h-3.5 w-3.5" />
                          </button>
                        </AlertDialogTrigger>
                        <AlertDialogContent onClick={(e) => e.stopPropagation()}>
                          <AlertDialogHeader>
                            <AlertDialogTitle className="text-red-600">确认删除该社群的本地数据</AlertDialogTitle>
                            <AlertDialogDescription className="text-red-700">
                              此操作将删除该社群的本地数据库、下载文件与图片缓存，不会影响账号对该社群的访问权限。操作不可恢复。
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel onClick={(e) => e.stopPropagation()}>取消</AlertDialogCancel>
                            <AlertDialogAction
                              className="bg-red-600 hover:bg-red-700 focus:ring-red-600"
                              onClick={(e) => {
                                e.stopPropagation();
                                handleDeleteGroup(group.group_id);
                              }}
                            >
                              确认删除
                            </AlertDialogAction>
                          </AlertDialogFooter>
                        </AlertDialogContent>
                      </AlertDialog>
                    </div>
                  </div>
                </div>
              );
            })}
              </div>
            )}
          </TabsContent>

          {/* 本地群组 */}
          <TabsContent value="local">
            {localGroups.length === 0 ? (
              <Card className="max-w-md mx-auto border border-gray-200 shadow-none">
                <CardContent className="pt-6">
                  <div className="text-center">
                    <p className="text-muted-foreground">
                      暂无本地群组，请先执行采集或从旧版本迁移数据
                    </p>
                  </div>
                </CardContent>
              </Card>
            ) : (
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 xl:grid-cols-6 gap-4">
                {localGroups.map((group) => {
                  const stats = groupStats[group.group_id];
                  return (
                    <div
                      key={group.group_id}
                      className="group-card cursor-pointer bg-white border border-gray-200 rounded-lg hover:border-gray-300 transition-all duration-200 hover:shadow-md overflow-hidden w-[200px]"
                      onClick={() => router.push(`/groups/${group.group_id}`)}
                    >
                      {/* 群组封面：固定200x200 */}
                      <div className="w-[200px] h-[200px]">
                        <SafeImage
                          src={group.background_url}
                          alt={group.name}
                          className="w-full h-full object-cover"
                          fallbackClassName="w-full h-full bg-gradient-to-br"
                          fallbackText={group.name.slice(0, 2)}
                          fallbackGradient={getGradientByType(group.type)}
                        />
                      </div>

                      {/* 内容区域 */}
                      <div className="p-2.5">
                        {/* 群组名称 */}
                        <h3 className="text-sm font-semibold text-gray-900 line-clamp-1 mb-1.5">
                          {group.name}
                        </h3>

                        {/* 统计信息 */}
                        <div className="flex items-center justify-between text-xs text-gray-500 mb-1.5">
                          {/* 群主信息 */}
                          {group.owner && (
                            <div className="flex items-center gap-1">
                              <Crown className="h-3 w-3" />
                              <span className="truncate max-w-[60px]">{group.owner.name}</span>
                            </div>
                          )}

                          {/* 话题数量 */}
                          {stats && (
                            <div className="flex items-center gap-1">
                              <MessageSquare className="h-3 w-3" />
                              <span>{stats.topics_count || 0}</span>
                            </div>
                          )}
                        </div>

                        {/* 类型标识和删除 */}
                        <div className="flex items-center justify-between">
                          <Badge variant="secondary" className="text-xs px-1.5 py-0 h-5">
                            本地
                          </Badge>

                          {/* 删除按钮 */}
                          <AlertDialog>
                            <AlertDialogTrigger asChild>
                              <button
                                type="button"
                                onClick={(e) => { e.stopPropagation(); }}
                                className="p-1 text-gray-400 hover:text-red-600 transition-colors"
                                title="删除本地数据"
                                disabled={deletingGroups.has(group.group_id)}
                              >
                                <Trash2 className="h-3.5 w-3.5" />
                              </button>
                            </AlertDialogTrigger>
                            <AlertDialogContent onClick={(e) => e.stopPropagation()}>
                              <AlertDialogHeader>
                                <AlertDialogTitle className="text-red-600">确认删除该社群的本地数据</AlertDialogTitle>
                                <AlertDialogDescription className="text-red-700">
                                  此操作将删除该社群的本地数据库、下载文件与图片缓存，不会影响账号对该社群的访问权限。操作不可恢复。
                                </AlertDialogDescription>
                              </AlertDialogHeader>
                              <AlertDialogFooter>
                                <AlertDialogCancel onClick={(e) => e.stopPropagation()}>取消</AlertDialogCancel>
                                <AlertDialogAction
                                  className="bg-red-600 hover:bg-red-700 focus:ring-red-600"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    handleDeleteGroup(group.group_id);
                                  }}
                                >
                                  确认删除
                                </AlertDialogAction>
                              </AlertDialogFooter>
                            </AlertDialogContent>
                          </AlertDialog>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
