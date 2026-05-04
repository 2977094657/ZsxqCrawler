'use client';

import { ChangeEvent, useRef, useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Badge } from '@/components/ui/badge';
import { AlertDialog, AlertDialogAction, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '@/components/ui/alert-dialog';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Loader2, Upload } from 'lucide-react';
import { apiClient, ImportPreview } from '@/lib/api';
import ImportGroupPreviewCard from './ImportGroupPreviewCard';
import { toast } from 'sonner';

interface ConfigPanelProps {
  onConfigSaved: () => void;
}

export default function ConfigPanel({ onConfigSaved }: ConfigPanelProps) {
  const [loading, setLoading] = useState(false);
  const [cookie, setCookie] = useState('');
  const [showInstructions, setShowInstructions] = useState(false);
  const [importPreviewing, setImportPreviewing] = useState(false);
  const [importing, setImporting] = useState(false);
  const [importFile, setImportFile] = useState<File | null>(null);
  const [importPreview, setImportPreview] = useState<ImportPreview | null>(null);
  const [importDialogOpen, setImportDialogOpen] = useState(false);
  const importInputRef = useRef<HTMLInputElement | null>(null);

  const handleSaveConfig = async () => {
    if (!cookie.trim()) {
      toast.error('请填写完整的 Cookie');
      return;
    }

    try {
      setLoading(true);
      await apiClient.updateConfig({
        cookie: cookie.trim(),
      });
      
      toast.success('配置保存成功！');
      onConfigSaved();
    } catch (error) {
      toast.error(`配置保存失败: ${error instanceof Error ? error.message : '未知错误'}`);
    } finally {
      setLoading(false);
    }
  };

  const formatBytes = (bytes?: number) => {
    if (!bytes || bytes <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let value = bytes;
    let index = 0;
    while (value >= 1024 && index < units.length - 1) {
      value /= 1024;
      index += 1;
    }
    return `${value.toFixed(value >= 10 || index === 0 ? 0 : 1)} ${units[index]}`;
  };

  const formatDateTime = (dateString?: string) => {
    if (!dateString) return '';
    try {
      return new Date(dateString).toLocaleString('zh-CN');
    } catch {
      return dateString;
    }
  };

  const handleImportFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = '';
    if (!file) return;
    if (!file.name.toLowerCase().endsWith('.zip')) {
      toast.error('请选择 zip 文件');
      return;
    }
    setImportFile(file);
    setImportPreview(null);
    setImportPreviewing(true);
    try {
      const preview = await apiClient.previewImportArchive(file);
      setImportPreview(preview);
      setImportDialogOpen(true);
      if (preview.conflicts.length > 0) {
        toast.warning('导入包中存在已本地存在的社群，请先删除后再导入');
      }
    } catch (error) {
      toast.error(`读取导入包失败: ${error instanceof Error ? error.message : '未知错误'}`);
      setImportFile(null);
    } finally {
      setImportPreviewing(false);
    }
  };

  const handleConfirmImport = async () => {
    if (!importFile || importing || !importPreview?.can_import) return;
    setImporting(true);
    try {
      const result = await apiClient.confirmImportArchive(importFile);
      toast.success(result.message || '导入成功');
      setImportDialogOpen(false);
      setImportFile(null);
      setImportPreview(null);
      onConfigSaved();
    } catch (error) {
      toast.error(`导入失败: ${error instanceof Error ? error.message : '未知错误'}`);
    } finally {
      setImporting(false);
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto p-4">
        <div className="mb-4">
          <p className="text-sm text-muted-foreground">
            请选择导入已有数据，或配置知识星球 Cookie 后开始使用
          </p>
        </div>

        <div className="max-w-5xl mx-auto space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Badge variant="secondary">📦</Badge>
                导入已有数据
              </CardTitle>
              <CardDescription>
                如果您已经有导出的 zip 数据包，可以先导入本地社群数据，无需立即填写 Cookie
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <input
                ref={importInputRef}
                type="file"
                accept=".zip,application/zip"
                className="hidden"
                onChange={handleImportFileChange}
              />
              <Button
                variant="outline"
                onClick={() => importInputRef.current?.click()}
                disabled={importPreviewing || importing}
                className="w-full"
              >
                {importPreviewing ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Upload className="h-4 w-4" />
                )}
                {importPreviewing ? '读取清单中...' : '选择 zip 导入'}
              </Button>
              <p className="text-xs text-muted-foreground">
                导入前会显示导出时间、数据大小和社群列表；如果本地已有同名社群数据，系统会拒绝导入。
              </p>
            </CardContent>
          </Card>

          {/* 配置表单 */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Badge variant="secondary">⚙️</Badge>
                配置认证信息
              </CardTitle>
              <CardDescription>
                填写您的知识星球 Cookie，后端会自动获取该账号下的全部星球
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="cookie">知识星球Cookie</Label>
                <Textarea
                  id="cookie"
                  placeholder="请粘贴完整的Cookie值..."
                  value={cookie}
                  onChange={(e) => setCookie(e.target.value)}
                  rows={3}
                />
                <p className="text-xs text-muted-foreground">
                  从浏览器开发者工具的Network标签中复制完整的Cookie值
                </p>
              </div>

              <div className="flex gap-2">
                <Button
                  onClick={handleSaveConfig}
                  disabled={loading || !cookie.trim()}
                  className="flex-1"
                >
                  {loading ? '保存中...' : '保存配置'}
                </Button>
                <Button
                  variant="outline"
                  onClick={() => setShowInstructions(true)}
                >
                  📖 查看详细说明
                </Button>
              </div>
            </CardContent>
          </Card>
          </div>

          {/* 快速测试 */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Badge variant="secondary">🧪</Badge>
                测试配置
              </CardTitle>
              <CardDescription>
                保存配置后可以测试连接是否正常
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Button
                variant="outline"
                onClick={() => window.open('http://localhost:8208/docs', '_blank')}
                className="w-full"
              >
                📖 查看API文档
              </Button>
            </CardContent>
          </Card>
        </div>

        <Dialog open={importDialogOpen} onOpenChange={setImportDialogOpen}>
          <DialogContent className="sm:max-w-2xl">
            <DialogHeader>
              <DialogTitle>确认导入数据包</DialogTitle>
              <DialogDescription>
                导入前请确认压缩包中的导出时间、数据大小和社群列表。已存在的社群不会被覆盖。
              </DialogDescription>
            </DialogHeader>
            {importPreview && (
              <div className="space-y-3">
                <div className="grid grid-cols-3 gap-2">
                  <div className="rounded-lg border border-border p-2.5">
                    <div className="text-xs text-muted-foreground mb-1">导出时间</div>
                    <div className="text-sm font-medium">
                      {formatDateTime(importPreview.manifest.exported_at) || importPreview.manifest.exported_at}
                    </div>
                  </div>
                  <div className="rounded-lg border border-border p-2.5">
                    <div className="text-xs text-muted-foreground mb-1">数据大小</div>
                    <div className="text-sm font-medium">{formatBytes(importPreview.manifest.data_size_bytes)}</div>
                  </div>
                  <div className="rounded-lg border border-border p-2.5">
                    <div className="text-xs text-muted-foreground mb-1">社群数量</div>
                    <div className="text-sm font-medium">{importPreview.manifest.groups_count}</div>
                  </div>
                </div>

                {importPreview.conflicts.length > 0 && (
                  <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-3">
                    <div className="text-sm font-medium text-destructive mb-1">存在冲突，无法导入</div>
                    <div className="text-xs text-muted-foreground">
                      以下社群的本地文件夹已存在，请先删除已有本地数据后再导入。
                    </div>
                    <div className="mt-2 flex flex-wrap gap-1.5">
                      {importPreview.conflicts.map((conflict) => (
                        <Badge key={conflict.group_id} variant="destructive">
                          ID: {conflict.group_id}
                        </Badge>
                      ))}
                    </div>
                  </div>
                )}

                <div className="rounded-lg border border-border">
                  <div className="px-3 py-2 text-sm font-medium border-b border-border">
                    将导入的社群
                  </div>
                  <div className="max-h-72 overflow-y-auto p-2.5">
                    <div className="grid grid-cols-1 gap-2">
                    {importPreview.groups.map((group) => (
                      <ImportGroupPreviewCard
                        key={group.group_id}
                        group={group}
                        conflicted={importPreview.conflicts.some((conflict) => conflict.group_id === group.group_id)}
                      />
                    ))}
                    </div>
                  </div>
                </div>
              </div>
            )}
            <DialogFooter>
              <Button
                variant="outline"
                onClick={() => {
                  setImportDialogOpen(false);
                  setImportFile(null);
                  setImportPreview(null);
                }}
                disabled={importing}
              >
                取消
              </Button>
              <Button
                onClick={handleConfirmImport}
                disabled={importing || !importPreview?.can_import}
              >
                {importing && <Loader2 className="h-4 w-4 animate-spin" />}
                确认导入
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        {/* 详细说明对话框 */}
        <AlertDialog open={showInstructions} onOpenChange={setShowInstructions}>
          <AlertDialogContent className="max-w-4xl max-h-[80vh] overflow-y-auto">
            <AlertDialogHeader>
              <AlertDialogTitle>📖 详细配置说明</AlertDialogTitle>
              <AlertDialogDescription>
                按照以下步骤获取所需的认证信息
              </AlertDialogDescription>
            </AlertDialogHeader>
            
            <div className="space-y-6">
              {/* Cookie获取说明 */}
              <div className="space-y-3">
                <h3 className="text-lg font-semibold">1. 获取Cookie</h3>
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 space-y-2">
                  <ol className="list-decimal list-inside space-y-2 text-sm">
                    <li>使用Chrome或Edge浏览器访问 <code className="bg-muted px-1 rounded">https://wx.zsxq.com/</code></li>
                    <li>登录您的知识星球账号</li>
                    <li>按 <kbd className="bg-muted border border-border px-2 py-1 rounded">F12</kbd> 打开开发者工具</li>
                    <li>切换到 <strong>Network</strong> (网络) 标签</li>
                    <li>刷新页面或点击任意链接</li>
                    <li>在网络请求列表中找到任意一个请求（通常是API请求）</li>
                    <li>点击该请求，在右侧面板中找到 <strong>Request Headers</strong></li>
                    <li>找到 <code className="bg-muted px-1 rounded">Cookie:</code> 行，复制完整的值</li>
                  </ol>
                </div>
              </div>

              {/* 不再需要在配置文件中填写群组ID，登录后将在前端选择具体星球 */}

              {/* 注意事项 */}
              <div className="space-y-3">
                <h3 className="text-lg font-semibold">⚠️ 注意事项</h3>
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 space-y-2">
                  <ul className="list-disc list-inside space-y-1 text-sm">
                    <li>Cookie包含您的登录凭证，请妥善保管，不要泄露给他人</li>
                    <li>Cookie有时效性，如果采集失败可能需要重新获取</li>
                    <li>确保您有权限访问目标知识星球群组</li>
                    <li>请遵守知识星球的使用条款和相关法律法规</li>
                    <li>本工具仅供学习和研究使用</li>
                  </ul>
                </div>
              </div>
            </div>

            <AlertDialogFooter>
              <AlertDialogAction onClick={() => setShowInstructions(false)}>
                我知道了
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      </div>
    </div>
  );
}
