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
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from '@/components/ui/dialog';
import { toast } from 'sonner';

interface AccountWithInfo extends Account {
  selfInfo?: AccountSelf | null;
  loadingSelf?: boolean;
}

export default function AccountPanel() {
  const [accounts, setAccounts] = useState<AccountWithInfo[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

  // 添加账号弹窗
  const [creating, setCreating] = useState<boolean>(false);
  const [createOpen, setCreateOpen] = useState<boolean>(false);
  const [name, setName] = useState<string>('');
  const [cookie, setCookie] = useState<string>('');
  const [makeDefault, setMakeDefault] = useState<boolean>(false);

  const loadAccounts = async () => {
    setLoading(true);
    try {
      const res = await apiClient.listAccounts();
      const list: AccountWithInfo[] = (res.accounts || []).map(acc => ({ ...acc, loadingSelf: true }));
      setAccounts(list);
      
      // 并发加载所有账号的自我信息
      const promises = list.map(async (acc) => {
        try {
          const selfRes = await apiClient.getAccountSelf(acc.id);
          return { id: acc.id, selfInfo: selfRes?.self || null };
        } catch {
          return { id: acc.id, selfInfo: null };
        }
      });
      
      const results = await Promise.all(promises);
      
      // 更新账号列表，填入自我信息
      setAccounts(prev => prev.map(acc => {
        const result = results.find(r => r.id === acc.id);
        return { ...acc, selfInfo: result?.selfInfo || null, loadingSelf: false };
      }));
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
      setCreateOpen(false);
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

  const handleRefresh = async (id: string) => {
    // 标记该账号为刷新中
    setAccounts(prev => prev.map(acc => 
      acc.id === id ? { ...acc, loadingSelf: true } : acc
    ));
    
    try {
      const res = await apiClient.refreshAccountSelf(id);
      setAccounts(prev => prev.map(acc => 
        acc.id === id ? { ...acc, selfInfo: res?.self || null, loadingSelf: false } : acc
      ));
      toast.success('信息已刷新');
    } catch (e: any) {
      toast.error(`刷新失败: ${e?.message || '未知错误'}`);
      setAccounts(prev => prev.map(acc => 
        acc.id === id ? { ...acc, loadingSelf: false } : acc
      ));
    }
  };

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <div>
            <CardTitle>账号管理</CardTitle>
            <CardDescription>表格展示账号信息，支持设为默认、刷新与删除操作</CardDescription>
          </div>
          <div>
            <Button variant="default" onClick={() => setCreateOpen(true)}>添加账号</Button>
          </div>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="text-sm text-muted-foreground">加载中...</div>
          ) : accounts.length === 0 ? (
            <div className="text-sm text-muted-foreground">暂无账号，请先添加</div>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>账号名称</TableHead>
                    <TableHead>用户信息</TableHead>
                    <TableHead>UID</TableHead>
                    <TableHead>位置</TableHead>
                    <TableHead>Cookie</TableHead>
                    <TableHead>默认</TableHead>
                    <TableHead>创建时间</TableHead>
                    <TableHead className="text-right">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {accounts.map((acc) => (
                    <TableRow key={acc.id}>
                      <TableCell className="font-medium">{acc.name || acc.id}</TableCell>
                      <TableCell>
                        {acc.loadingSelf ? (
                          <span className="text-xs text-gray-400">加载中...</span>
                        ) : acc.selfInfo ? (
                          <div className="flex items-center gap-2">
                            {acc.selfInfo.avatar_url && (
                              <img
                                src={apiClient.getProxyImageUrl(acc.selfInfo.avatar_url)}
                                alt={acc.selfInfo.name || ''}
                                className="w-6 h-6 rounded-full"
                                onError={(e) => { (e.currentTarget as HTMLImageElement).style.display = 'none'; }}
                              />
                            )}
                            <div className="text-sm">
                              <div className="font-medium">{acc.selfInfo.name || '未命名'}</div>
                              {acc.selfInfo.grade && (
                                <div className="text-xs text-gray-500">{acc.selfInfo.grade}</div>
                              )}
                            </div>
                          </div>
                        ) : (
                          <span className="text-xs text-gray-400">无信息</span>
                        )}
                      </TableCell>
                      <TableCell className="text-sm text-gray-600">
                        {acc.selfInfo?.uid || '-'}
                      </TableCell>
                      <TableCell className="text-sm text-gray-600">
                        {acc.selfInfo?.location || '-'}
                      </TableCell>
                      <TableCell className="max-w-[200px] truncate text-xs text-gray-500">
                        {acc.cookie || '***'}
                      </TableCell>
                      <TableCell>
                        {acc.is_default ? <Badge variant="secondary">默认</Badge> : <span className="text-gray-400">-</span>}
                      </TableCell>
                      <TableCell className="text-sm text-gray-600">{acc.created_at || '-'}</TableCell>
                      <TableCell className="text-right space-x-2">
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => handleRefresh(acc.id)}
                          disabled={acc.loadingSelf}
                        >
                          {acc.loadingSelf ? '刷新中...' : '刷新'}
                        </Button>
                        {!acc.is_default && acc.id !== 'default' && (
                          <Button size="sm" variant="outline" onClick={() => handleSetDefault(acc.id)}>
                            设为默认
                          </Button>
                        )}
                        {acc.id !== 'default' && (
                          <Button size="sm" variant="destructive" onClick={() => handleDelete(acc.id)}>
                            删除
                          </Button>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>添加新账号</DialogTitle>
            <DialogDescription>仅保存 Cookie 与名称，Cookie 将被安全掩码展示</DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
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
          <DialogFooter className="mt-4">
            <Button variant="outline" onClick={() => setCreateOpen(false)}>取消</Button>
            <Button onClick={handleCreate} disabled={creating || !cookie.trim()} className="min-w-24">
              {creating ? '提交中...' : '添加账号'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}