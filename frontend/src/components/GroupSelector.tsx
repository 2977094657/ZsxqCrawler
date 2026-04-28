'use client';

import { useState, useEffect, useMemo } from 'react';
import { useRouter } from 'next/navigation';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { MessageSquare, Crown, UserCog, RefreshCw, Trash2, Loader2 } from 'lucide-react';
import { apiClient, Group, GroupStats } from '@/lib/api';
import { toast } from 'sonner';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog';
import { cn } from '@/lib/utils';
import SafeImage from './SafeImage';
import '../styles/group-selector.css';

interface GroupSelectorProps {
  // 选中群组后的回调；当前组件直接使用 router.push 进行导航，因此此回调可选保留以兼容上游用法。
  onGroupSelected?: (group: Group) => void;
}

type SourceFilter = 'all' | 'account' | 'local';

export default function GroupSelector(_props: GroupSelectorProps) {
  const router = useRouter();
  const [groups, setGroups] = useState<Group[]>([]);
  const [groupStats, setGroupStats] = useState<Record<number, GroupStats>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retryCount, setRetryCount] = useState(0);
  const [isRetrying, setIsRetrying] = useState(false);
  const [deletingGroups, setDeletingGroups] = useState<Set<number>>(new Set());
  // 当前激活的来源筛选标签
  const [sourceFilter, setSourceFilter] = useState<SourceFilter>('all');
  // 用户点击某个群组卡片后正在跳转到详情页的群组 id；用于立即显示全屏加载遮罩
  const [navigatingTo, setNavigatingTo] = useState<{ id: number; name: string } | null>(null);

  useEffect(() => {
    loadGroups();
  }, []);

  // 监听页面可见性变化和窗口焦点，返回页面时自动刷新群组列表
  // 使用节流避免频繁刷新
  useEffect(() => {
    let lastRefresh = 0;
    const REFRESH_INTERVAL = 5000; // 最少间隔 5 秒

    const maybeRefresh = () => {
      // 用户从详情页返回时，重置导航中遮罩状态，避免遮罩残留
      setNavigatingTo(null);

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
    // pageshow 在浏览器使用 BFCache 恢复页面时触发（visibilitychange 不会触发），
    // 这里也一并处理，保证从详情页回退时遮罩一定被清除。
    const handlePageShow = () => {
      setNavigatingTo(null);
    };
    document.addEventListener('visibilitychange', handleVisibilityChange);
    window.addEventListener('focus', handleFocus);
    window.addEventListener('pageshow', handlePageShow);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange);
      window.removeEventListener('focus', handleFocus);
      window.removeEventListener('pageshow', handlePageShow);
    };
  }, []);

  // 按来源对群组分类。一个群可能同时存在于账号侧与本地（source: "account|local"），
  // 此时它既算"网络"也算"本地"。
  // 注意：所有 useMemo 必须在任何条件 return 之前调用，否则 React 会在加载/正常渲染之间
  // 改变 hook 顺序，触发 "change in the order of Hooks" 报错。

  // 排序规则：网络群组（当前账号可访问，包含 source 中含 'account' 或 source 为空的）优先；
  // 仅本地存在的群组排后面；同优先级内保持原有顺序（来自后端的次序）。
  const sortedGroups = useMemo(() => {
    const hasAccount = (g: Group) => !g.source || g.source.includes('account');
    return [...groups].sort((a, b) => {
      const aAcc = hasAccount(a);
      const bAcc = hasAccount(b);
      if (aAcc === bAcc) return 0;
      return aAcc ? -1 : 1;
    });
  }, [groups]);

  const accountGroups = useMemo(
    () => sortedGroups.filter((g) => !g.source || g.source.includes('account')),
    [sortedGroups]
  );
  const localGroups = useMemo(
    () => sortedGroups.filter((g) => g.source && g.source.includes('local')),
    [sortedGroups]
  );
  // 根据当前选中的标签过滤群组
  const filteredGroups = useMemo(() => {
    if (sourceFilter === 'account') return accountGroups;
    if (sourceFilter === 'local') return localGroups;
    return sortedGroups;
  }, [sourceFilter, sortedGroups, accountGroups, localGroups]);

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

      // 注：原本会为每个群组拉取 /v3/users/self 信息，但该数据未在本组件渲染中使用，
      // 同时会导致每次进入首页都为 N 个群组各发一次 self 请求，徒增后端负担
      // 并使账号管理页切换变慢。已删除此预热逻辑——
      // 如需此信息，请在真正展示它的页面（如群组详情页）按需获取。

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



  // 当筛选后的群组列表变化时，预取详情页路由的 JS chunk，
  // 减少首次点击时的"白屏等待"时间。Next.js 的 router.prefetch 是幂等的，重复调用代价很低。
  useEffect(() => {
    if (filteredGroups.length === 0) return;
    // 仅预取前 24 个，避免极端情况下一次性发太多请求
    filteredGroups.slice(0, 24).forEach((g) => {
      try {
        router.prefetch(`/groups/${g.group_id}`);
      } catch {
        // prefetch 失败不影响功能
      }
    });
  }, [filteredGroups, router]);

  // 点击群组卡片后立即显示加载遮罩，并跳转到详情页。
  // 这样即使详情页本身首次加载较慢，用户也能立即看到反馈。
  const handleSelectGroup = (group: Group) => {
    if (navigatingTo) return; // 防止重复点击
    setNavigatingTo({ id: group.group_id, name: group.name });
    router.push(`/groups/${group.group_id}`);
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
                <p className="text-xs text-muted-foreground/70 mt-2">
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
            <p className="text-sm text-muted-foreground">
              加载群组列表时出现错误
            </p>
          </div>
          <Card className="max-w-md mx-auto">
            <CardHeader>
              <CardTitle className="text-destructive">加载失败</CardTitle>
              <CardDescription>无法获取群组列表</CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground mb-4">{error}</p>
              <Button onClick={() => loadGroups(0)} className="w-full">
                重试
              </Button>
            </CardContent>
          </Card>
        </div>
      </div>
    );
  }

  // 渲染单个群组卡片，避免重复代码
  const renderGroupCard = (group: Group) => {
    const stats = groupStats[group.group_id];
    const hasAccount = !group.source || group.source.includes('account');
    const hasLocal = !!group.source && group.source.includes('local');

    const isNavigating = navigatingTo?.id === group.group_id;

    return (
      <div
        key={group.group_id}
        className={cn(
          'group-card cursor-pointer bg-card border border-border rounded-lg hover:border-primary/40 transition-colors overflow-hidden w-full relative',
          isNavigating && 'opacity-80'
        )}
        onClick={() => handleSelectGroup(group)}
      >
        {/* 群组封面：随卡片宽度自适应的正方形 */}
        <div className="w-full aspect-square border-b border-border">
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
          <h3 className="text-sm font-semibold text-foreground line-clamp-1 mb-1.5">
            {group.name}
          </h3>

          {/* 统计信息：群主名字右侧紧跟"本地"标签 */}
          <div className="flex items-center justify-between text-xs text-muted-foreground mb-1.5 gap-1.5">
            <div className="flex items-center gap-1 min-w-0 flex-1">
              {group.owner && (
                <>
                  <Crown className="h-3 w-3 shrink-0" />
                  <span className="truncate">{group.owner.name}</span>
                </>
              )}
              {hasLocal && (
                <Badge
                  variant="outline"
                  className="text-[10px] leading-none px-1 py-0 h-4 font-normal text-primary border-primary/30 shrink-0"
                >
                  本地
                </Badge>
              )}
            </div>
            {stats && (
              <div className="flex items-center gap-1 shrink-0">
                <MessageSquare className="h-3 w-3" />
                <span>{stats.topics_count || 0}</span>
              </div>
            )}
          </div>

          {/* 来源标签 + 状态标签 + 删除按钮 */}
          <div className="flex items-center justify-between gap-1.5">
            <div className="flex items-center gap-1 flex-wrap">
              {/* 来源指示标签：只保留"网络"，"本地"已移至群主名字右侧 */}
              {hasAccount && (
                <Badge variant="outline" className="text-xs px-1.5 py-0 h-5 font-normal">
                  网络
                </Badge>
              )}

              {/* 付费状态（仅网络群组适用） */}
              {hasAccount && (group.type === 'pay' ? (
                group.status === 'expired' ? (
                  <Badge variant="destructive" className="text-xs px-1.5 py-0 h-5">
                    已过期
                  </Badge>
                ) : isExpiringWithinMonth(group.expiry_time) ? (
                  <Badge variant="outline" className="text-xs px-1.5 py-0 h-5 text-yellow-600 border-yellow-200">
                    即将过期
                  </Badge>
                ) : (
                  <Badge
                    variant="outline"
                    className={cn(
                      'text-xs px-1.5 py-0 h-5 font-normal',
                      group.is_trial
                        ? 'text-purple-600 border-purple-200'
                        : 'text-green-600 border-green-200'
                    )}
                  >
                    {group.is_trial ? '试用' : '付费'}
                  </Badge>
                )
              ) : (
                group.type !== 'local' && (
                  <Badge variant="outline" className="text-xs px-1.5 py-0 h-5 font-normal text-muted-foreground">
                    免费
                  </Badge>
                )
              ))}
            </div>

            {/* 删除按钮 */}
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <button
                  type="button"
                  onClick={(e) => { e.stopPropagation(); }}
                  className="p-1 text-muted-foreground/70 hover:text-destructive transition-colors"
                  title="删除本地数据"
                  disabled={deletingGroups.has(group.group_id)}
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              </AlertDialogTrigger>
              <AlertDialogContent onClick={(e) => e.stopPropagation()}>
                <AlertDialogHeader>
                  <AlertDialogTitle className="text-destructive">确认删除该社群的本地数据</AlertDialogTitle>
                  <AlertDialogDescription>
                    此操作将删除该社群的本地数据库、下载文件与图片缓存，不会影响账号对该社群的访问权限。操作不可恢复。
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel onClick={(e) => e.stopPropagation()}>取消</AlertDialogCancel>
                  <AlertDialogAction
                    className="bg-destructive text-white hover:bg-destructive/90 focus-visible:ring-destructive/30"
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
  };

  // 顶部筛选标签按钮
  const filterTags: { key: SourceFilter; label: string; count: number }[] = [
    { key: 'all', label: '全部', count: groups.length },
    { key: 'account', label: '网络群组', count: accountGroups.length },
    { key: 'local', label: '本地群组', count: localGroups.length },
  ];

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto p-4">
        {/* 页面标题 + 操作 */}
        <div className="mb-4">
          <div className="flex items-center justify-between">
            <div>
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

        {/* 来源筛选标签：使用细线框 chip 样式替代 Tabs */}
        <div className="flex flex-wrap items-center gap-2 mb-4">
          {filterTags.map((tag) => {
            const active = sourceFilter === tag.key;
            return (
              <button
                key={tag.key}
                type="button"
                onClick={() => setSourceFilter(tag.key)}
                className={cn(
                  // Claude 风格：与卡片/按钮一致的圆角，激活态使用柔和的浅橙背景
                  'inline-flex items-center gap-1.5 h-8 px-3 rounded-lg border text-xs font-medium transition-colors',
                  active
                    ? 'bg-primary/10 text-primary border-primary/30'
                    : 'bg-card text-foreground border-border hover:border-primary/30 hover:bg-accent'
                )}
              >
                <span>{tag.label}</span>
                <span
                  className={cn(
                    'inline-flex items-center justify-center min-w-[1.25rem] h-4 px-1 rounded-md text-[10px]',
                    active
                      ? 'bg-primary/20 text-primary'
                      : 'bg-muted text-muted-foreground'
                  )}
                >
                  {tag.count}
                </span>
              </button>
            );
          })}
        </div>

        {/* 群组网格 */}
        {filteredGroups.length === 0 ? (
          <Card className="max-w-md mx-auto">
            <CardContent className="pt-6">
              <div className="text-center">
                <p className="text-muted-foreground text-sm">
                  {sourceFilter === 'account'
                    ? '暂无可访问的网络群组，请先在账号管理中添加或更新 Cookie'
                    : sourceFilter === 'local'
                    ? '暂无本地群组，请先执行采集或从旧版本迁移数据'
                    : '暂无群组，请先添加账号或采集本地数据'}
                </p>
              </div>
            </CardContent>
          </Card>
        ) : (
          // 响应式列数：卡片宽度随容器均分，gap 始终保持一致，不会出现“右侧留白”问题。
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-5">
            {filteredGroups.map(renderGroupCard)}
          </div>
        )}
      </div>

      {/* 全屏导航加载遮罩：点击群组卡片后立即显示，给用户即时反馈，
          避免详情页首屏 chunk 下载或后端 /api/groups 请求时出现"无反应"的错觉。 */}
      {navigatingTo && (
        <div
          role="status"
          aria-live="polite"
          className="fixed inset-0 z-50 flex items-center justify-center bg-background/70 backdrop-blur-sm"
        >
          <div className="flex flex-col items-center gap-3 px-6 py-5 rounded-xl border border-border bg-card shadow-lg">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
            <div className="text-sm font-medium text-foreground">
              正在打开「{navigatingTo.name}」
            </div>
            <div className="text-xs text-muted-foreground">加载社群数据中…</div>
          </div>
        </div>
      )}
    </div>
  );
}
