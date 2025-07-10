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

interface CrawlSettingsDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  crawlInterval: number;
  longSleepInterval: number;
  pagesPerBatch: number;
  onSettingsChange: (settings: {
    crawlInterval: number;
    longSleepInterval: number;
    pagesPerBatch: number;
    crawlIntervalMin?: number;
    crawlIntervalMax?: number;
    longSleepIntervalMin?: number;
    longSleepIntervalMax?: number;
  }) => void;
}

export default function CrawlSettingsDialog({
  open,
  onOpenChange,
  crawlInterval,
  longSleepInterval,
  pagesPerBatch,
  onSettingsChange,
}: CrawlSettingsDialogProps) {
  const [localCrawlInterval, setLocalCrawlInterval] = useState(crawlInterval);
  const [localLongSleepInterval, setLocalLongSleepInterval] = useState(longSleepInterval);
  const [localPagesPerBatch, setLocalPagesPerBatch] = useState(pagesPerBatch);

  // 新增范围设置状态
  const [crawlIntervalMin, setCrawlIntervalMin] = useState(2);
  const [crawlIntervalMax, setCrawlIntervalMax] = useState(5);
  const [longSleepIntervalMin, setLongSleepIntervalMin] = useState(180);
  const [longSleepIntervalMax, setLongSleepIntervalMax] = useState(300);
  const [useRandomInterval, setUseRandomInterval] = useState(true);
  const [selectedPreset, setSelectedPreset] = useState<'fast' | 'standard' | 'safe' | null>('standard');

  // 当对话框打开时，同步当前设置值
  useEffect(() => {
    if (open) {
      setLocalCrawlInterval(crawlInterval);
      setLocalLongSleepInterval(longSleepInterval);
      setLocalPagesPerBatch(pagesPerBatch);

      // 如果是第一次打开，默认设置为标准配置
      if (crawlInterval === 3.5 && longSleepInterval === 240 && pagesPerBatch === 15) {
        setPreset('standard');
      }
    }
  }, [open, crawlInterval, longSleepInterval, pagesPerBatch]);

  const handleSave = () => {
    // 确保所有值都有默认值
    const finalCrawlIntervalMin = crawlIntervalMin || 2;
    const finalCrawlIntervalMax = crawlIntervalMax || 5;
    const finalLongSleepIntervalMin = longSleepIntervalMin || 180;
    const finalLongSleepIntervalMax = longSleepIntervalMax || 300;
    const finalPagesPerBatch = Math.max(localPagesPerBatch || 15, 5); // 确保最小值为5



    const settingsToSend = {
      crawlInterval: useRandomInterval
        ? (finalCrawlIntervalMin + finalCrawlIntervalMax) / 2
        : Math.round((finalCrawlIntervalMin + finalCrawlIntervalMax) / 2),
      longSleepInterval: useRandomInterval
        ? (finalLongSleepIntervalMin + finalLongSleepIntervalMax) / 2
        : Math.round((finalLongSleepIntervalMin + finalLongSleepIntervalMax) / 2),
      pagesPerBatch: finalPagesPerBatch,
      crawlIntervalMin: useRandomInterval ? finalCrawlIntervalMin : undefined,
      crawlIntervalMax: useRandomInterval ? finalCrawlIntervalMax : undefined,
      longSleepIntervalMin: useRandomInterval ? finalLongSleepIntervalMin : undefined,
      longSleepIntervalMax: useRandomInterval ? finalLongSleepIntervalMax : undefined,
    };

    try {
      onSettingsChange(settingsToSend);
    } catch (error) {
      console.error('CrawlSettingsDialog handleSave - onSettingsChange 调用失败:', error);
    }

    onOpenChange(false);
  };

  const handleCancel = () => {
    // 重置为原始值
    setLocalCrawlInterval(crawlInterval);
    setLocalLongSleepInterval(longSleepInterval);
    setLocalPagesPerBatch(pagesPerBatch);
    onOpenChange(false);
  };

  const setPreset = (preset: 'fast' | 'standard' | 'safe') => {
    setUseRandomInterval(true);
    setSelectedPreset(preset);
    switch (preset) {
      case 'fast':
        setCrawlIntervalMin(1);
        setCrawlIntervalMax(3);
        setLongSleepIntervalMin(60);
        setLongSleepIntervalMax(120);
        setLocalPagesPerBatch(20);
        break;
      case 'standard':
        setCrawlIntervalMin(2);
        setCrawlIntervalMax(5);
        setLongSleepIntervalMin(180);
        setLongSleepIntervalMax(300);
        setLocalPagesPerBatch(15);
        break;
      case 'safe':
        setCrawlIntervalMin(5);
        setCrawlIntervalMax(10);
        setLongSleepIntervalMin(300);
        setLongSleepIntervalMax(600);
        setLocalPagesPerBatch(10);
        break;
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[500px]">
        <DialogHeader>
          <DialogTitle>话题爬取间隔设置</DialogTitle>
          <DialogDescription>
            调整话题爬取的间隔时间和批次设置，以避免触发反爬虫机制。
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

          {/* 爬取间隔范围 */}
          <div className="space-y-2">
            <Label>页面爬取间隔范围 (秒)</Label>
            <div className="flex gap-2 items-center">
              <Input
                type="number"
                min="1"
                max="60"
                value={crawlIntervalMin}
                onChange={(e) => {
                  const value = e.target.value;
                  if (value === '') {
                    setCrawlIntervalMin('');
                  } else {
                    const num = parseInt(value);
                    if (!isNaN(num)) {
                      setCrawlIntervalMin(num);
                    }
                  }
                }}
                onBlur={(e) => {
                  if (e.target.value === '') {
                    setCrawlIntervalMin(2);
                  }
                }}
                placeholder="2"
                className="flex-1"
              />
              <span className="text-sm text-gray-500">-</span>
              <Input
                type="number"
                min="1"
                max="60"
                value={crawlIntervalMax}
                onChange={(e) => {
                  const value = e.target.value;
                  if (value === '') {
                    setCrawlIntervalMax('');
                  } else {
                    const num = parseInt(value);
                    if (!isNaN(num)) {
                      setCrawlIntervalMax(num);
                    }
                  }
                }}
                onBlur={(e) => {
                  if (e.target.value === '') {
                    setCrawlIntervalMax(5);
                  }
                }}
                placeholder="5"
                className="flex-1"
              />
            </div>
            <p className="text-xs text-gray-500">
              {useRandomInterval
                ? '每次爬取页面后的随机等待时间范围'
                : `每次爬取页面后的固定等待时间 (取中间值: ${Math.round((crawlIntervalMin + crawlIntervalMax) / 2)}秒)`
              }
            </p>
          </div>

          {/* 长休眠间隔范围 */}
          <div className="space-y-2">
            <Label>长休眠间隔范围 (秒)</Label>
            <div className="flex gap-2 items-center">
              <Input
                type="number"
                min="60"
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
                    setLongSleepIntervalMin(180);
                  }
                }}
                placeholder="180"
                className="flex-1"
              />
              <span className="text-sm text-gray-500">-</span>
              <Input
                type="number"
                min="60"
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
                    setLongSleepIntervalMax(300);
                  }
                }}
                placeholder="300"
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
            <Label>批次大小 (页面数)</Label>
            <Input
              type="number"
              min="5"
              max="50"
              value={localPagesPerBatch}
              onChange={(e) => {
                const value = e.target.value;
                if (value === '') {
                  setLocalPagesPerBatch('');
                } else {
                  const num = parseInt(value);
                  if (!isNaN(num)) {
                    setLocalPagesPerBatch(num);
                  }
                }
              }}
              onBlur={(e) => {
                if (e.target.value === '' || parseInt(e.target.value) < 5) {
                  setLocalPagesPerBatch(15);
                }
              }}
              placeholder="15"
            />
            <p className="text-xs text-gray-500">
              爬取多少个页面后触发长休眠（最小值：5页）
            </p>
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
              <div>• 快速: 1-3秒间隔, 1-2分钟长休眠, 20页/批次</div>
              <div>• 标准: 2-5秒间隔, 3-5分钟长休眠, 15页/批次</div>
              <div>• 安全: 5-10秒间隔, 5-10分钟长休眠, 10页/批次</div>
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
