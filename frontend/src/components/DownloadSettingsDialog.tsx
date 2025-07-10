'use client';

import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';

interface DownloadSettingsDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  downloadInterval: number;
  longSleepInterval: number;
  filesPerBatch: number;
  onSettingsChange: (settings: {
    downloadInterval: number;
    longSleepInterval: number;
    filesPerBatch: number;
    downloadIntervalMin?: number;
    downloadIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
  }) => void;
}

export default function DownloadSettingsDialog({
  open,
  onOpenChange,
  downloadInterval,
  longSleepInterval,
  filesPerBatch,
  onSettingsChange,
}: DownloadSettingsDialogProps) {
  const [localDownloadInterval, setLocalDownloadInterval] = useState(downloadInterval);
  const [localLongSleepInterval, setLocalLongSleepInterval] = useState(longSleepInterval);
  const [localFilesPerBatch, setLocalFilesPerBatch] = useState(filesPerBatch);

  // 新增范围设置状态
  const [downloadIntervalMin, setDownloadIntervalMin] = useState(15);
  const [downloadIntervalMax, setDownloadIntervalMax] = useState(30);
  const [longSleepIntervalMin, setLongSleepIntervalMin] = useState(30);
  const [longSleepIntervalMax, setLongSleepIntervalMax] = useState(60);
  const [useRandomInterval, setUseRandomInterval] = useState(true);
  const [selectedPreset, setSelectedPreset] = useState<'fast' | 'standard' | 'safe' | null>('fast');

  // 当对话框打开时，同步当前设置值
  useEffect(() => {
    if (open) {
      setLocalDownloadInterval(downloadInterval);
      setLocalLongSleepInterval(longSleepInterval);
      setLocalFilesPerBatch(filesPerBatch);

      // 如果是第一次打开，默认设置为快速配置
      if (downloadInterval === 1.0 && longSleepInterval === 60.0 && filesPerBatch === 10) {
        setPreset('fast');
      }
    }
  }, [open, downloadInterval, longSleepInterval, filesPerBatch]);

  const handleSave = () => {
    // 确保所有值都有默认值
    const finalDownloadIntervalMin = downloadIntervalMin || 15;
    const finalDownloadIntervalMax = downloadIntervalMax || 30;
    const finalLongSleepIntervalMin = longSleepIntervalMin || 30;
    const finalLongSleepIntervalMax = longSleepIntervalMax || 60;
    const finalFilesPerBatch = localFilesPerBatch || 10;

    onSettingsChange({
      downloadInterval: useRandomInterval
        ? (finalDownloadIntervalMin + finalDownloadIntervalMax) / 2
        : Math.round((finalDownloadIntervalMin + finalDownloadIntervalMax) / 2),
      longSleepInterval: useRandomInterval
        ? (finalLongSleepIntervalMin + finalLongSleepIntervalMax) / 2
        : Math.round((finalLongSleepIntervalMin + finalLongSleepIntervalMax) / 2),
      filesPerBatch: finalFilesPerBatch,
      downloadIntervalMin: useRandomInterval ? finalDownloadIntervalMin : undefined,
      downloadIntervalMax: useRandomInterval ? finalDownloadIntervalMax : undefined,
      longSleepIntervalMin: useRandomInterval ? finalLongSleepIntervalMin : undefined,
      longSleepIntervalMax: useRandomInterval ? finalLongSleepIntervalMax : undefined,
    });
    onOpenChange(false);
  };

  const handleCancel = () => {
    // 重置为原始值
    setLocalDownloadInterval(downloadInterval);
    setLocalLongSleepInterval(longSleepInterval);
    setLocalFilesPerBatch(filesPerBatch);
    onOpenChange(false);
  };

  const setPreset = (preset: 'fast' | 'standard' | 'safe') => {
    setUseRandomInterval(true);
    setSelectedPreset(preset);
    switch (preset) {
      case 'fast':
        setDownloadIntervalMin(15);
        setDownloadIntervalMax(30);
        setLongSleepIntervalMin(30);
        setLongSleepIntervalMax(60);
        setLocalFilesPerBatch(30);
        break;
      case 'standard':
        setDownloadIntervalMin(30);
        setDownloadIntervalMax(60);
        setLongSleepIntervalMin(60);
        setLongSleepIntervalMax(180);
        setLocalFilesPerBatch(15);
        break;
      case 'safe':
        setDownloadIntervalMin(60);
        setDownloadIntervalMax(180);
        setLongSleepIntervalMin(180);
        setLongSleepIntervalMax(300);
        setLocalFilesPerBatch(5);
        break;
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[500px]">
        <DialogHeader>
          <DialogTitle>下载间隔设置</DialogTitle>
          <DialogDescription>
            调整文件下载的间隔时间和批次设置，以避免触发反爬虫机制。
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          {/* 间隔模式选择 */}
          <div className="space-y-2">
            <Label>间隔模式</Label>
            <div className="flex gap-2">
              <Button
                type="button"
                variant={useRandomInterval ? "default" : "outline"}
                size="sm"
                onClick={() => setUseRandomInterval(true)}
                className="flex-1"
              >
                随机间隔 (推荐)
              </Button>
              <Button
                type="button"
                variant={!useRandomInterval ? "default" : "outline"}
                size="sm"
                onClick={() => setUseRandomInterval(false)}
                className="flex-1"
              >
                固定间隔
              </Button>
            </div>
          </div>

          {/* 下载间隔范围 */}
          <div className="space-y-2">
            <Label>下载间隔范围 (秒)</Label>
            <div className="flex gap-2 items-center">
              <Input
                type="number"
                min="1"
                max="300"
                value={downloadIntervalMin}
                onChange={(e) => {
                  const value = e.target.value;
                  if (value === '') {
                    setDownloadIntervalMin('');
                  } else {
                    const num = parseInt(value);
                    if (!isNaN(num)) {
                      setDownloadIntervalMin(num);
                    }
                  }
                }}
                onBlur={(e) => {
                  if (e.target.value === '') {
                    setDownloadIntervalMin(15);
                  }
                }}
                placeholder="15"
                className="flex-1"
              />
              <span className="text-sm text-gray-500">-</span>
              <Input
                type="number"
                min="1"
                max="300"
                value={downloadIntervalMax}
                onChange={(e) => {
                  const value = e.target.value;
                  if (value === '') {
                    setDownloadIntervalMax('');
                  } else {
                    const num = parseInt(value);
                    if (!isNaN(num)) {
                      setDownloadIntervalMax(num);
                    }
                  }
                }}
                onBlur={(e) => {
                  if (e.target.value === '') {
                    setDownloadIntervalMax(30);
                  }
                }}
                placeholder="30"
                className="flex-1"
              />
            </div>
            <p className="text-xs text-gray-500">
              {useRandomInterval
                ? '每次下载文件后的随机等待时间范围'
                : `每次下载文件后的固定等待时间 (取中间值: ${Math.round((downloadIntervalMin + downloadIntervalMax) / 2)}秒)`
              }
            </p>
          </div>

          {/* 长休眠间隔范围 */}
          <div className="space-y-2">
            <Label>长休眠间隔范围 (秒)</Label>
            <div className="flex gap-2 items-center">
              <Input
                type="number"
                min="10"
                max="3600"
                value={longSleepIntervalMin}
                onChange={(e) => {
                  const value = e.target.value;
                  if (value === '') {
                    setLongSleepIntervalMin('');
                  } else {
                    const num = parseInt(value);
                    if (!isNaN(num)) {
                      setLongSleepIntervalMin(num);
                    }
                  }
                }}
                onBlur={(e) => {
                  if (e.target.value === '') {
                    setLongSleepIntervalMin(30);
                  }
                }}
                placeholder="30"
                className="flex-1"
              />
              <span className="text-sm text-gray-500">-</span>
              <Input
                type="number"
                min="10"
                max="3600"
                value={longSleepIntervalMax}
                onChange={(e) => {
                  const value = e.target.value;
                  if (value === '') {
                    setLongSleepIntervalMax('');
                  } else {
                    const num = parseInt(value);
                    if (!isNaN(num)) {
                      setLongSleepIntervalMax(num);
                    }
                  }
                }}
                onBlur={(e) => {
                  if (e.target.value === '') {
                    setLongSleepIntervalMax(60);
                  }
                }}
                placeholder="60"
                className="flex-1"
              />
            </div>
            <p className="text-xs text-gray-500">
              {useRandomInterval
                ? '达到批次大小后的随机长时间休眠范围'
                : `达到批次大小后的固定长时间休眠 (取中间值: ${Math.round((longSleepIntervalMin + longSleepIntervalMax) / 2)}秒)`
              }
            </p>
          </div>

          {/* 批次大小 */}
          <div className="space-y-2">
            <Label htmlFor="filesPerBatch">批次大小 (个文件)</Label>
            <Input
              id="filesPerBatch"
              type="number"
              min="1"
              max="100"
              step="1"
              value={localFilesPerBatch}
              onChange={(e) => {
                const value = e.target.value;
                if (value === '') {
                  setLocalFilesPerBatch('');
                } else {
                  const num = parseInt(value);
                  if (!isNaN(num)) {
                    setLocalFilesPerBatch(num);
                  }
                }
              }}
              onBlur={(e) => {
                if (e.target.value === '') {
                  setLocalFilesPerBatch(10);
                }
              }}
              placeholder="10"
            />
            <p className="text-xs text-gray-500">下载多少个文件后触发长休眠</p>
          </div>

          {/* 快速配置 */}
          <div className="space-y-2">
            <Label>快速配置</Label>
            <div className="flex gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setPreset('fast')}
                className={`flex-1 ${
                  selectedPreset === 'fast'
                    ? 'bg-green-100 text-green-800 border-green-300 hover:bg-green-200'
                    : 'bg-gray-50 text-gray-500 border-gray-200 hover:bg-gray-100'
                }`}
              >
                快速
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setPreset('standard')}
                className={`flex-1 ${
                  selectedPreset === 'standard'
                    ? 'bg-blue-100 text-blue-800 border-blue-300 hover:bg-blue-200'
                    : 'bg-gray-50 text-gray-500 border-gray-200 hover:bg-gray-100'
                }`}
              >
                标准
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => setPreset('safe')}
                className={`flex-1 ${
                  selectedPreset === 'safe'
                    ? 'bg-orange-100 text-orange-800 border-orange-300 hover:bg-orange-200'
                    : 'bg-gray-50 text-gray-500 border-gray-200 hover:bg-gray-100'
                }`}
              >
                安全
              </Button>
            </div>
            <div className="text-xs text-gray-500 space-y-1">
              <div>• 快速: 15-30秒间隔, 30秒-1分钟长休眠, 30个文件/批次</div>
              <div>• 标准: 30秒-1分钟间隔, 1-3分钟长休眠, 15个文件/批次</div>
              <div>• 安全: 1-3分钟间隔, 3-5分钟长休眠, 5个文件/批次</div>
            </div>
          </div>
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" onClick={handleCancel}>
            取消
          </Button>
          <Button type="button" onClick={handleSave}>
            保存设置
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
