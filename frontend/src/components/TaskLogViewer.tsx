'use client';

import { useState, useEffect, useRef } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { X, Minimize2, Maximize2, Terminal, MessageSquare, Square, AlertTriangle } from 'lucide-react';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';

interface TaskLogViewerProps {
  taskId: string | null;
  onClose: () => void;
  isMinimized?: boolean;
  onToggleMinimize?: () => void;
  inline?: boolean;
  onTaskStop?: () => void;
}

interface LogMessage {
  type: 'log' | 'status' | 'heartbeat';
  message?: string;
  status?: string;
}

export default function TaskLogViewer({
  taskId,
  onClose,
  isMinimized = false,
  onToggleMinimize,
  inline = false,
  onTaskStop
}: TaskLogViewerProps) {
  const [logs, setLogs] = useState<string[]>([]);
  const [status, setStatus] = useState<string>('pending');
  const [isConnected, setIsConnected] = useState(false);
  const [stopping, setStopping] = useState(false);
  const [showExpiredDialog, setShowExpiredDialog] = useState(false);
  const [expiredMessage, setExpiredMessage] = useState('');
  const scrollAreaRef = useRef<HTMLDivElement>(null);
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    if (!taskId) return;

    // 建立SSE连接
    const API_BASE_URL = 'http://localhost:8208';
    const eventSource = new EventSource(`${API_BASE_URL}/api/tasks/${taskId}/stream`);
    eventSourceRef.current = eventSource;

    eventSource.onopen = () => {
      setIsConnected(true);
      console.log('SSE连接已建立');
    };

    eventSource.onmessage = (event) => {
      try {
        const data: LogMessage = JSON.parse(event.data);
        
        if (data.type === 'log' && data.message) {
          setLogs(prev => [...prev, data.message!]);

          // 检测过期相关的日志消息
          if (data.message.includes('会员已过期') || data.message.includes('成员体验已到期')) {
            setExpiredMessage(data.message);
            setShowExpiredDialog(true);
          }
        } else if (data.type === 'status' && data.status) {
          setStatus(data.status);
          // 不再将状态信息添加到日志中，只更新状态

          // 如果任务完成或失败，关闭SSE连接
          if (data.status === 'completed' || data.status === 'failed') {
            console.log(`任务${data.status}，关闭SSE连接`);
            eventSource.close();
            setIsConnected(false);

            // 调用任务停止回调
            if (onTaskStop) {
              onTaskStop();
            }
          }
        }
        // heartbeat 不需要处理
      } catch (error) {
        console.error('解析SSE消息失败:', error);
      }
    };

    eventSource.onerror = (error) => {
      console.error('SSE连接错误:', error);
      setIsConnected(false);
    };

    return () => {
      eventSource.close();
      eventSourceRef.current = null;
    };
  }, [taskId]);

  // 监听状态变化，确保任务完成时关闭连接
  useEffect(() => {
    if ((status === 'completed' || status === 'failed' || status === 'cancelled') && eventSourceRef.current) {
      console.log(`状态变为${status}，确保SSE连接已关闭`);
      eventSourceRef.current.close();
      eventSourceRef.current = null;
      setIsConnected(false);
    }
  }, [status]);

  // 自动滚动到底部
  useEffect(() => {
    if (scrollAreaRef.current) {
      const scrollContainer = scrollAreaRef.current.querySelector('[data-radix-scroll-area-viewport]');
      if (scrollContainer) {
        scrollContainer.scrollTop = scrollContainer.scrollHeight;
      }
    }
  }, [logs]);

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'bg-gradient-to-r from-emerald-100 to-green-100 text-emerald-800 border border-emerald-200';
      case 'running':
        return 'bg-gradient-to-r from-blue-100 to-indigo-100 text-blue-800 border border-blue-200';
      case 'failed':
        return 'bg-gradient-to-r from-red-100 to-rose-100 text-red-800 border border-red-200';
      case 'pending':
        return 'bg-gradient-to-r from-amber-100 to-yellow-100 text-amber-800 border border-amber-200';
      case 'idle':
        return 'bg-gradient-to-r from-gray-100 to-slate-100 text-gray-600 border border-gray-300';
      default:
        return 'bg-gradient-to-r from-gray-100 to-slate-100 text-gray-800 border border-gray-200';
    }
  };

  const getStatusText = (status: string) => {
    switch (status) {
      case 'completed':
        return '已完成';
      case 'running':
        return '运行中';
      case 'failed':
        return '失败';
      case 'pending':
        return '等待中';
      default:
        return status;
    }
  };

  // 日志解析函数
  const getLogType = (log: string): string => {
    // 优先匹配具体的日志模式
    if (log.includes('🚀') || log.includes('开始') || log.includes('初始化')) return 'start';
    if (log.includes('✅') || log.includes('完成') || log.includes('成功')) return 'success';
    if (log.includes('❌') || log.includes('失败') || log.includes('错误') || log.includes('异常')) return 'error';
    if (log.includes('📊') || log.includes('累计') || log.includes('统计') || log.includes('进度报告')) return 'stats';
    if (log.includes('💾') || log.includes('存储') || log.includes('数据库') || log.includes('页面存储')) return 'storage';
    if (log.includes('⏰') || log.includes('时间') || log.includes('时间范围')) return 'time';
    if (log.includes('🔍') || log.includes('调试') || log.includes('详细信息')) return 'debug';
    if (log.includes('⚠️') || log.includes('警告') || log.includes('跳过')) return 'warning';
    if (log.includes('📡') || log.includes('连接') || log.includes('API') || log.includes('请求')) return 'network';
    if (log.includes('🎉') || log.includes('总结') || log.includes('最终状态')) return 'summary';
    if (log.includes('📄') || log.includes('页数') || log.includes('话题')) return 'progress';
    if (log.includes('[状态]')) return 'status';
    return 'info';
  };

  const cleanLogContent = (log: string): string => {
    // 移除时间戳前缀 [HH:MM:SS]
    return log.replace(/^\[\d{2}:\d{2}:\d{2}\]\s*/, '');
  };

  const extractTimestamp = (log: string): string => {
    // 提取时间戳 [HH:MM:SS]
    const match = log.match(/^\[(\d{2}:\d{2}:\d{2})\]/);
    return match ? match[1] : new Date().toLocaleTimeString();
  };

  const handleStopTask = async () => {
    if (!taskId || stopping) return;

    try {
      setStopping(true);
      const { apiClient } = await import('@/lib/api');
      await apiClient.stopTask(taskId);

      if (onTaskStop) {
        onTaskStop();
      }
    } catch (error) {
      console.error('停止任务失败:', error);
    } finally {
      setStopping(false);
    }
  };

  const getLogIcon = (type: string): string => {
    switch (type) {
      case 'start': return '🚀';
      case 'success': return '✅';
      case 'error': return '❌';
      case 'stats': return '📊';
      case 'storage': return '💾';
      case 'time': return '⏰';
      case 'debug': return '🔍';
      case 'warning': return '⚠️';
      case 'network': return '📡';
      case 'summary': return '🎉';
      case 'progress': return '📄';
      case 'status': return '🔄';
      default: return 'ℹ️';
    }
  };

  const getLogStyle = (type: string): string => {
    switch (type) {
      case 'start': return 'border-blue-400 bg-blue-50';
      case 'success': return 'border-green-400 bg-green-50';
      case 'error': return 'border-red-400 bg-red-50';
      case 'stats': return 'border-purple-400 bg-purple-50';
      case 'storage': return 'border-indigo-400 bg-indigo-50';
      case 'time': return 'border-orange-400 bg-orange-50';
      case 'debug': return 'border-gray-400 bg-gray-50';
      case 'warning': return 'border-yellow-400 bg-yellow-50';
      case 'network': return 'border-cyan-400 bg-cyan-50';
      case 'summary': return 'border-emerald-400 bg-emerald-50';
      case 'progress': return 'border-teal-400 bg-teal-50';
      case 'status': return 'border-sky-400 bg-sky-50';
      default: return 'border-gray-300 bg-gray-50';
    }
  };

  // 移除这个检查，让组件在没有taskId时也能显示框架

  // inline模式的渲染
  if (inline) {
    return (
      <>
        <div className="h-full flex flex-col p-3">
        {/* 状态栏 */}
        <div className="flex items-center justify-between p-3 bg-white rounded-lg border border-gray-200 mb-3">
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <div className={`w-2 h-2 rounded-full ${
                taskId && isConnected ? 'bg-green-500' : 'bg-gray-400'
              }`} />
              <span className="text-sm text-gray-600">状态:</span>
              <Badge className={`text-xs px-2 py-1 ${getStatusColor(taskId ? status : 'idle')}`}>
                {taskId ? getStatusText(status) : '空闲'}
              </Badge>
            </div>
            {/* 停止按钮 */}
            {taskId && status === 'running' && (
              <Button
                variant="destructive"
                size="sm"
                onClick={handleStopTask}
                disabled={stopping}
                className="h-6 px-2 text-xs"
              >
                <Square className="h-3 w-3 mr-1" />
                {stopping ? '停止中' : '停止'}
              </Button>
            )}
          </div>
          <div className="text-xs text-gray-500 font-mono">
            {taskId ? `${taskId.slice(0, 8)}...` : '无任务'}
          </div>
        </div>

        {/* 日志内容 */}
        <div className="flex-1 bg-white rounded-lg border border-gray-200 overflow-hidden flex flex-col">
          <div className="bg-gray-50 px-3 py-2 border-b border-gray-200 flex-shrink-0">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Terminal className="h-3 w-3 text-gray-500" />
                <span className="text-xs font-medium text-gray-600">日志</span>
              </div>
              <span className="text-xs text-gray-500">{taskId ? `${logs.length} 条` : '等待任务'}</span>
            </div>
          </div>
          <ScrollArea className="flex-1 w-full" ref={scrollAreaRef}>
            <div className="p-3 space-y-1">
              {!taskId ? (
                <div className="text-gray-500 text-center py-12">
                  <div className="w-16 h-16 mx-auto mb-4 bg-gray-100 rounded-full flex items-center justify-center">
                    <MessageSquare className="h-8 w-8 text-gray-400" />
                  </div>
                  <p className="font-medium text-gray-600 mb-1">暂无运行中的任务</p>
                  <p className="text-sm text-gray-500">执行爬取操作后将在此显示实时日志</p>
                </div>
              ) : logs.length === 0 ? (
                <div className="text-gray-500 text-center py-12">
                  <div className="w-16 h-16 mx-auto mb-4 bg-gray-100 rounded-full flex items-center justify-center">
                    <MessageSquare className="h-8 w-8 text-gray-400" />
                  </div>
                  <p className="font-medium text-gray-600 mb-1">等待任务开始</p>
                  <p className="text-sm text-gray-500">日志将在任务执行时实时显示</p>
                </div>
              ) : (
                logs.map((log, index) => {
                  // 解析日志类型和内容
                  const logType = getLogType(log);
                  const logContent = cleanLogContent(log);
                  const timestamp = extractTimestamp(log);

                  return (
                    <div key={index} className={`text-sm py-1 px-3 rounded border-l-2 ${getLogStyle(logType)} hover:bg-opacity-80 transition-colors`}>
                      <div className="flex items-center gap-3">
                        <div className="text-xs text-gray-500 font-mono flex-shrink-0 w-16">
                          {timestamp}
                        </div>
                        <div className="text-gray-900 flex-1 min-w-0">
                          {logContent}
                        </div>
                      </div>
                    </div>
                  );
                })
              )}
              {taskId && status === 'running' && (
                <div className="flex items-center gap-3 text-blue-600 py-1 px-3 bg-blue-50 rounded border-l-2 border-blue-400">
                  <div className="text-xs text-gray-500 font-mono flex-shrink-0 w-16">
                    {new Date().toLocaleTimeString()}
                  </div>
                  <div className="flex items-center gap-2 flex-1">
                    <div className="w-2 h-2 bg-blue-600 rounded-full animate-pulse"></div>
                    <span className="text-sm">任务正在执行中...</span>
                  </div>
                </div>
              )}
            </div>
          </ScrollArea>
        </div>

        {/* 过期提示对话框 */}
        <AlertDialog open={showExpiredDialog} onOpenChange={setShowExpiredDialog}>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle className="flex items-center gap-2">
                <AlertTriangle className="h-5 w-5 text-amber-500" />
                知识星球会员已过期
              </AlertDialogTitle>
              <AlertDialogDescription>
                {expiredMessage || '您的知识星球会员体验已到期，无法继续进行数据采集操作。请续费后重试。'}
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogAction onClick={() => setShowExpiredDialog(false)}>
                我知道了
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      </div>
      </>
    );
  }

  // 原有的浮动窗口模式
  return (
    <Card className={`fixed bottom-4 right-4 w-96 border border-gray-300 shadow-none z-50 ${
      isMinimized ? 'h-12' : 'h-80'
    } transition-all duration-200`}>
      <CardHeader className="pb-2 px-3 py-2 bg-gray-900 text-white rounded-t-lg">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Terminal className="h-4 w-4" />
            <CardTitle className="text-sm font-mono">
              任务日志 - {taskId?.slice(0, 8) || '未知任务'}
            </CardTitle>
          </div>
          <div className="flex items-center gap-2">
            <Badge className={`text-xs px-2 py-0 ${getStatusColor(status)}`}>
              {getStatusText(status)}
            </Badge>
            <div className={`w-2 h-2 rounded-full ${
              isConnected ? 'bg-green-400' : 'bg-red-400'
            }`} title={isConnected ? '已连接' : '未连接'} />
            {onToggleMinimize && (
              <Button
                variant="ghost"
                size="sm"
                className="h-6 w-6 p-0 text-white hover:bg-gray-700"
                onClick={onToggleMinimize}
              >
                {isMinimized ? <Maximize2 className="h-3 w-3" /> : <Minimize2 className="h-3 w-3" />}
              </Button>
            )}
            <Button
              variant="ghost"
              size="sm"
              className="h-6 w-6 p-0 text-white hover:bg-gray-700"
              onClick={onClose}
            >
              <X className="h-3 w-3" />
            </Button>
          </div>
        </div>
      </CardHeader>
      
      {!isMinimized && (
        <CardContent className="p-0 bg-black text-green-400 font-mono text-xs">
          <ScrollArea className="h-64 w-full" ref={scrollAreaRef}>
            <div className="p-3 space-y-1">
              {logs.length === 0 ? (
                <div className="text-gray-500">等待日志输出...</div>
              ) : (
                logs.map((log, index) => (
                  <div key={index} className="whitespace-pre-wrap break-words">
                    {log}
                  </div>
                ))
              )}
              {status === 'running' && (
                <div className="flex items-center gap-1 text-blue-400">
                  <span className="animate-pulse">●</span>
                  <span>任务运行中...</span>
                </div>
              )}
            </div>
          </ScrollArea>
        </CardContent>
      )}

      {/* 过期提示对话框 */}
      <AlertDialog open={showExpiredDialog} onOpenChange={setShowExpiredDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-amber-500" />
              知识星球会员已过期
            </AlertDialogTitle>
            <AlertDialogDescription>
              {expiredMessage || '您的知识星球会员体验已到期，无法继续进行数据采集操作。请续费后重试。'}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogAction onClick={() => setShowExpiredDialog(false)}>
              我知道了
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </Card>
  );
}
