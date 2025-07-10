'use client';

import { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { Badge } from '@/components/ui/badge';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from '@/components/ui/alert-dialog';
import { apiClient } from '@/lib/api';
import { toast } from 'sonner';

interface ConfigPanelProps {
  onConfigSaved: () => void;
}

export default function ConfigPanel({ onConfigSaved }: ConfigPanelProps) {
  const [loading, setLoading] = useState(false);
  const [cookie, setCookie] = useState('');
  const [groupId, setGroupId] = useState('');
  const [showInstructions, setShowInstructions] = useState(false);

  const handleSaveConfig = async () => {
    if (!cookie.trim() || !groupId.trim()) {
      toast.error('请填写完整的Cookie和群组ID');
      return;
    }

    try {
      setLoading(true);
      const response = await apiClient.updateConfig({
        cookie: cookie.trim(),
        group_id: groupId.trim(),
        db_path: 'zsxq_interactive.db'
      });
      
      toast.success('配置保存成功！');
      onConfigSaved();
    } catch (error) {
      toast.error(`配置保存失败: ${error instanceof Error ? error.message : '未知错误'}`);
    } finally {
      setLoading(false);
    }
  };

  const extractGroupIdFromUrl = (url: string) => {
    const match = url.match(/group\/(\d+)/);
    if (match) {
      setGroupId(match[1]);
      toast.success('已自动提取群组ID');
    } else {
      toast.error('无法从URL中提取群组ID，请检查URL格式');
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto p-6">
        <div className="mb-8">
          <h1 className="text-4xl font-bold mb-2">🌟 知识星球数据采集器</h1>
          <p className="text-muted-foreground">
            请配置您的知识星球认证信息以开始使用
          </p>
        </div>

        <div className="max-w-2xl mx-auto space-y-6">
          {/* 配置表单 */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Badge variant="secondary">⚙️</Badge>
                配置认证信息
              </CardTitle>
              <CardDescription>
                填写您的知识星球Cookie和群组ID
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

              <div className="space-y-2">
                <Label htmlFor="group-id">群组ID</Label>
                <div className="flex gap-2">
                  <Input
                    id="group-id"
                    placeholder="例如: 123456789"
                    value={groupId}
                    onChange={(e) => setGroupId(e.target.value)}
                  />
                  <Button
                    variant="outline"
                    onClick={() => {
                      const url = prompt('请输入知识星球群组页面URL:');
                      if (url) extractGroupIdFromUrl(url);
                    }}
                  >
                    从URL提取
                  </Button>
                </div>
                <p className="text-xs text-muted-foreground">
                  从知识星球群组页面URL中提取的数字ID
                </p>
              </div>

              <div className="flex gap-2">
                <Button
                  onClick={handleSaveConfig}
                  disabled={loading || !cookie.trim() || !groupId.trim()}
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
                onClick={() => window.open('http://localhost:8000/docs', '_blank')}
                className="w-full"
              >
                📖 查看API文档
              </Button>
            </CardContent>
          </Card>
        </div>

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
                    <li>使用Chrome或Edge浏览器访问 <code className="bg-gray-100 px-1 rounded">https://wx.zsxq.com/</code></li>
                    <li>登录您的知识星球账号</li>
                    <li>按 <kbd className="bg-gray-200 px-2 py-1 rounded">F12</kbd> 打开开发者工具</li>
                    <li>切换到 <strong>Network</strong> (网络) 标签</li>
                    <li>刷新页面或点击任意链接</li>
                    <li>在网络请求列表中找到任意一个请求（通常是API请求）</li>
                    <li>点击该请求，在右侧面板中找到 <strong>Request Headers</strong></li>
                    <li>找到 <code className="bg-gray-100 px-1 rounded">Cookie:</code> 行，复制完整的值</li>
                  </ol>
                </div>
              </div>

              {/* 群组ID获取说明 */}
              <div className="space-y-3">
                <h3 className="text-lg font-semibold">2. 获取群组ID</h3>
                <div className="bg-green-50 border border-green-200 rounded-lg p-4 space-y-2">
                  <ol className="list-decimal list-inside space-y-2 text-sm">
                    <li>在知识星球中进入您要采集的目标群组</li>
                    <li>查看浏览器地址栏的URL</li>
                    <li>URL格式类似：<code className="bg-gray-100 px-1 rounded">https://wx.zsxq.com/group/123456789</code></li>
                    <li>其中的数字部分 <code className="bg-gray-100 px-1 rounded">123456789</code> 就是群组ID</li>
                    <li>或者点击上方的"从URL提取"按钮，粘贴完整URL自动提取</li>
                  </ol>
                </div>
              </div>

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
