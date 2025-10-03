'use client';

import React, { useEffect, useState } from 'react';
import { apiClient, Account, AccountSelf } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { toast } from 'sonner';

export default function AccountPanel() {
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [creating, setCreating] = useState<boolean>(false);
  const [name, setName] = useState<string>('');
  const [cookie, setCookie] = useState<string>('');
  const [makeDefault, setMakeDefault] = useState<boolean>(false);

  // 自我信息查看/刷新
  const [selectedInfoAccountId, setSelectedInfoAccountId] = useState<string | null>(null);
  const [selectedInfo, setSelectedInfo] = useState<AccountSelf | null>(null);
  const [loadingInfo, setLoadingInfo] = useState<boolean>(false);
  const [refreshingInfo, setRefreshingInfo] = useState<boolean>(false);

  const loadAccounts = async () => {
    setLoading(true);
    try {
      const res = await apiClient.listAccounts();
      setAccounts(res.accounts || []);
    } catch (e) {
      toast.error('加载账号列表失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadAccounts();
  }, []);

  const handleCreate = async () => {
    if (!cookie.trim()) {
      toast.error('请填写Cookie');
      return;
    }
    setCreating(true);
    try {
      await apiClient.createAccount({ cookie: cookie.trim(), name: name.trim() || undefined, make_default: makeDefault });
      toast.success('账号已添加');
      setCookie('');
      setName('');
      setMakeDefault(false);
      await loadAccounts();
    } catch (e: any) {
      toast.error(`添加失败: ${e?.message || '未知错误'}`);
    } finally {
      setCreating(false);
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm('确认删除该账号？删除后分配到此账号的群组将回退到默认账号。')) return;
    try {
      await apiClient.deleteAccount(id);
      toast.success('账号已删除');
      await loadAccounts();
    } catch (e: any) {
      toast.error(`删除失败: ${e?.message || '未知错误'}`);
    }
  };

  const handleSetDefault = async (id: string) => {
    try {
      await apiClient.setDefaultAccount(id);
      toast.success('已设为默认账号');
      await loadAccounts();
    } catch (e: any) {
      toast.error(`设置失败: ${e?.message || '未知错误'}`);
    }
  };

  // 加载/刷新指定账号的自我信息
  const fetchAccountSelf = async (id: string, refresh = false) => {
    try {
      setSelectedInfoAccountId(id);
      if (refresh) {
        setRefreshingInfo(true);
        const res = await apiClient.refreshAccountSelf(id);
        setSelectedInfo(res?.self || null);
      } else {
        setLoadingInfo(true);
        const res = await apiClient.getAccountSelf(id);
        setSelectedInfo(res?.self || null);
      }
    } catch (e: any) {
      toast.error(`获取账号信息失败: ${e?.message || '未知错误'}`);
    } finally {
      setLoadingInfo(false);
      setRefreshingInfo(false);
    }
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>添加新账号</CardTitle>
          <CardDescription>在此添加新的知识星球账号（仅保存 Cookie 与名称，Cookie将被安全掩码展示）</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-3">
            <div className="space-y-2">
              <Label htmlFor="acc-name">账号名称（可选）</Label>
              <Input id="acc-name" placeholder="例如：个人号/备用号" value={name} onChange={(e) => setName(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="acc-cookie">Cookie</Label>
              <Textarea
                id="acc-cookie"
                placeholder="粘贴完整的 Cookie 值..."
                rows={4}
                value={cookie}
                onChange={(e) => setCookie(e.target.value)}
              />
            </div>
            <div className="flex items-center gap-2">
              <input
                id="acc-default"
                type="checkbox"
                checked={makeDefault}
                onChange={(e) => setMakeDefault(e.target.checked)}
              />
              <Label htmlFor="acc-default">设为默认账号</Label>
            </div>
          </div>
          <div className="flex gap-2">
            <Button onClick={handleCreate} disabled={creating || !cookie.trim()} className="min-w-24">
              {creating ? '提交中...' : '添加账号'}
            </Button>
            <Button
              variant="outline"
              onClick={() => {
                setName('');
                setCookie('');
                setMakeDefault(false);
              }}
            >
              重置
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>账号列表</CardTitle>
          <CardDescription>删除或设为默认账号。群组与账号的绑定可在群组详情页查看与调整。</CardDescription>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="text-sm text-muted-foreground">加载中...</div>
          ) : accounts.length === 0 ? (
            <div className="text-sm text-muted-foreground">暂无账号，请先添加</div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>名称</TableHead>
                  <TableHead>Cookie（掩码）</TableHead>
                  <TableHead>默认</TableHead>
                  <TableHead>创建时间</TableHead>
                  <TableHead className="text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {accounts.map((acc) => (
                  <TableRow key={acc.id}>
                    <TableCell className="font-medium">{acc.name || acc.id}</TableCell>
                    <TableCell>{acc.cookie || '***'}</TableCell>
                    <TableCell>
                      {acc.is_default ? <Badge variant="secondary">默认</Badge> : <span className="text-gray-400">-</span>}
                    </TableCell>
                    <TableCell>{acc.created_at || '-'}</TableCell>
                    <TableCell className="text-right space-x-2">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => fetchAccountSelf(acc.id, false)}
                        disabled={loadingInfo && selectedInfoAccountId === acc.id}
                      >
                        {loadingInfo && selectedInfoAccountId === acc.id ? '加载中...' : '查看信息'}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => fetchAccountSelf(acc.id, true)}
                        disabled={refreshingInfo && selectedInfoAccountId === acc.id}
                      >
                        {refreshingInfo && selectedInfoAccountId === acc.id ? '刷新中...' : '刷新信息'}
                      </Button>
                      {!acc.is_default && (
                        <Button size="sm" variant="outline" onClick={() => handleSetDefault(acc.id)}>
                          设为默认
                        </Button>
                      )}
                      <Button size="sm" variant="destructive" onClick={() => handleDelete(acc.id)}>
                        删除
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
          {/* 选中账号的自我信息展示 */}
          {selectedInfo && (
            <div className="mt-4 p-3 border rounded-lg bg-gray-50">
              <div className="flex items-center gap-3">
                {selectedInfo.avatar_url && (
                  <img
                    src={apiClient.getProxyImageUrl(selectedInfo.avatar_url)}
                    alt={selectedInfo.name || ''}
                    className="w-10 h-10 rounded-full"
                    onError={(e) => { (e.currentTarget as HTMLImageElement).style.display = 'none'; }}
                  />
                )}
                <div className="text-sm">
                  <div className="font-medium">
                    {selectedInfo.name || '未命名用户'}
                    {selectedInfo.grade ? ` · ${selectedInfo.grade}` : ''}
                  </div>
                  <div className="text-gray-500">UID: {selectedInfo.uid || '-'}</div>
                  <div className="text-gray-500">位置: {selectedInfo.location || '-'}</div>
                  <div className="text-gray-400">更新于: {selectedInfo.fetched_at || '-'}</div>
                </div>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}