'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Users, MessageSquare, Crown, UserCog } from 'lucide-react';
import { apiClient, Group, GroupStats, AccountSelf } from '@/lib/api';
import { toast } from 'sonner';
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

  useEffect(() => {
    loadGroups();
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

      // 检查是否获取到有效数据
      if (!data || !data.groups || data.groups.length === 0) {
        throw new Error('API返回空数据，可能是反爬虫机制');
      }

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
        <div className="container mx-auto p-6">
          <div className="mb-8">
            <h1 className="text-4xl font-bold mb-2">🌟 知识星球数据采集器</h1>
            <p className="text-muted-foreground">
              {isRetrying ? '正在重试获取群组列表...' : '正在加载您的知识星球群组...'}
            </p>
          </div>
          <div className="flex items-center justify-center py-12">
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
        <div className="container mx-auto p-6">
          <div className="mb-8">
            <h1 className="text-4xl font-bold mb-2">🌟 知识星球数据采集器</h1>
            <p className="text-muted-foreground">
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

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto p-6">
        <div className="mb-8">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-4xl font-bold mb-2">🌟 知识星球数据采集器</h1>
              <p className="text-muted-foreground">
                选择要操作的知识星球群组
              </p>
            </div>
            <div className="flex items-center gap-2">
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
        <div className="mb-6">
          <p className="text-sm text-muted-foreground">
            共 {groups.length} 个群组
          </p>
        </div>

        {/* 群组网格 */}
        {groups.length === 0 ? (
          <Card className="max-w-md mx-auto border border-gray-200 shadow-none">
            <CardContent className="pt-6">
              <div className="text-center">
                <p className="text-muted-foreground">
                  暂无可访问的群组
                </p>
              </div>
            </CardContent>
          </Card>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {groups.map((group) => {
              const stats = groupStats[group.group_id];
              return (
                <Card
                  key={group.group_id}
                  className="cursor-pointer border border-gray-200 hover:border-gray-300 transition-all duration-200 shadow-none hover:shadow-sm"
                  onClick={() => router.push(`/groups/${group.group_id}`)}
                >
                  <CardContent className="p-4">
                    {/* 群组头像/背景图 */}
                    <div className="mb-4">
                      <SafeImage
                        src={group.background_url}
                        alt={group.name}
                        className="w-full h-32 rounded-lg object-cover"
                        fallbackClassName="w-full h-32 rounded-lg"
                        fallbackText={group.name.slice(0, 2)}
                        fallbackGradient={getGradientByType(group.type)}
                      />
                    </div>

                    {/* 群组名称 */}
                    <h3 className="text-lg font-semibold text-gray-900 line-clamp-2 mb-3 min-h-[3.5rem]">
                      {group.name}
                    </h3>

                    {/* 统计信息 */}
                    <div className="space-y-2 mb-4">
                      <div className="flex items-center justify-between text-sm">
                        {/* 群主信息 */}
                        {group.owner && (
                          <div className="flex items-center gap-1 text-gray-600">
                            <Crown className="h-4 w-4" />
                            <span className="truncate">{group.owner.name}</span>
                          </div>
                        )}

                        {/* 话题数量 */}
                        {stats && (
                          <div className="flex items-center gap-1 text-gray-600">
                            <MessageSquare className="h-4 w-4" />
                            <span>{stats.topics_count || 0}</span>
                          </div>
                        )}
                      </div>

                      {/* 所属账号标记（头像 + 名称） */}
                      <div className="flex items-center gap-2 text-xs text-gray-600">
                        {accountSelfMap[group.group_id]?.avatar_url ? (
                          <img
                            src={apiClient.getProxyImageUrl(accountSelfMap[group.group_id]!.avatar_url!)}
                            alt={accountSelfMap[group.group_id]?.name || ''}
                            className="w-5 h-5 rounded-full"
                            onError={(e) => { (e.currentTarget as HTMLImageElement).style.display = 'none'; }}
                          />
                        ) : (
                          <div className="w-5 h-5 rounded-full bg-gray-200" />
                        )}
                        <span className="truncate">
                          {accountSelfMap[group.group_id]?.name ||
                           (group.account?.name || group.account?.id) ||
                           '默认账号'}
                        </span>
                      </div>
                    </div>

                    {/* 类型标识和状态 */}
                    <div className="flex items-center justify-between">
                      {/* 根据付费状态和过期情况显示不同颜色 */}
                      {group.type === 'pay' ? (
                        // 付费群组：检查是否过期或即将过期
                        group.status === 'expired' ? (
                          <Badge variant="destructive" className="text-xs">
                            {group.is_trial ? '试用已过期' : '付费已过期'}
                          </Badge>
                        ) : isExpiringWithinMonth(group.expiry_time) ? (
                          <Badge variant="outline" className="text-xs text-yellow-600 border-yellow-200">
                            {group.is_trial ? '试用即将过期' : '付费即将过期'}
                          </Badge>
                        ) : (
                          <Badge className={`text-xs ${group.is_trial ? 'bg-purple-600 hover:bg-purple-700' : 'bg-green-600 hover:bg-green-700'}`}>
                            {group.is_trial ? '试用中' : '付费'}
                          </Badge>
                        )
                      ) : (
                        <Badge variant="secondary" className="text-xs">
                          {group.type === 'free' ? '免费' : group.type}
                        </Badge>
                      )}
                    </div>

                    {/* 时间信息 */}
                    <div className="mt-3 pt-3 border-t border-gray-100">
                      <div className="space-y-1 text-xs text-gray-500">
                        {group.join_time && (
                          <div>
                            加入时间: {formatDate(group.join_time)}
                          </div>
                        )}
                        {group.expiry_time && (
                          <div className={
                            group.status === 'expiring_soon' ? 'text-yellow-600' :
                            group.status === 'expired' ? 'text-red-600' : ''
                          }>
                            {group.is_trial ? '试用' : '会员'}过期时间: {formatDate(group.expiry_time)}
                          </div>
                        )}
                      </div>
                    </div>
                  </CardContent>
                </Card>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
