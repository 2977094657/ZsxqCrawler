'use client';

import React, { useEffect, useState } from 'react';
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

type Mode = 'latest' | 'range';

interface CrawlLatestDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirm: (params: {
    mode: Mode;
    startTime?: string;
    endTime?: string;
    lastDays?: number;
    perPage?: number;
  }) => void;
  submitting?: boolean;
  defaultLastDays?: number;
  defaultPerPage?: number;
}

export default function CrawlLatestDialog({
  open,
  onOpenChange,
  onConfirm,
  submitting = false,
  defaultLastDays = 7,
  defaultPerPage = 20,
}: CrawlLatestDialogProps) {
  const [mode, setMode] = useState<Mode>('latest');
  const [lastDays, setLastDays] = useState<number | ''>(defaultLastDays);
  const [startTime, setStartTime] = useState('');
  const [endTime, setEndTime] = useState('');
  const [perPage, setPerPage] = useState<number | ''>(defaultPerPage);

  useEffect(() => {
    if (open) {
      setMode('latest');
      setLastDays(defaultLastDays);
      setStartTime('');
      setEndTime('');
      setPerPage(defaultPerPage);
    }
  }, [open, defaultLastDays, defaultPerPage]);

  const handleConfirm = () => {
    const payload: {
      mode: Mode;
      startTime?: string;
      endTime?: string;
      lastDays?: number;
      perPage?: number;
    } = { mode };

    if (mode === 'range') {
      if (startTime) payload.startTime = new Date(startTime).toISOString();
      if (endTime) payload.endTime = new Date(endTime).toISOString();
      if (lastDays !== '' && !Number.isNaN(Number(lastDays))) {
        payload.lastDays = Number(lastDays);
      }
      if (perPage !== '' && !Number.isNaN(Number(perPage))) {
        payload.perPage = Number(perPage);
      }
    }

    onConfirm(payload);
  };

  const isConfirmDisabled =
    submitting || (mode === 'range' && lastDays === '' && !startTime && !endTime);

  return (
    <Dialog open={open} onOpenChange={(v) => !submitting && onOpenChange(v)}>
      <DialogContent className="sm:max-w-[560px]">
        <DialogHeader>
          <DialogTitle>获取最新话题</DialogTitle>
          <DialogDescription>
            默认从最新开始抓取；也可以按时间区间采集，首次数据库为空也可使用。
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2">
          <div className="space-y-2">
            <Label>采集方式</Label>
            <div className="flex gap-2">
              <Button
                type="button"
                variant={mode === 'latest' ? 'default' : 'outline'}
                size="sm"
                onClick={() => setMode('latest')}
                className="flex-1"
              >
                从最新开始（默认）
              </Button>
              <Button
                type="button"
                variant={mode === 'range' ? 'default' : 'outline'}
                size="sm"
                onClick={() => setMode('range')}
                className="flex-1"
              >
                按时间区间
              </Button>
            </div>
          </div>

          {mode === 'range' ? (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label>最近天数（可选）</Label>
                <Input
                  type="number"
                  min="1"
                  max="3650"
                  placeholder={`${defaultLastDays}`}
                  value={lastDays}
                  onChange={(e) => {
                    const v = e.target.value;
                    if (v === '') setLastDays('');
                    else {
                      const n = parseInt(v);
                      if (!Number.isNaN(n)) setLastDays(n);
                    }
                  }}
                />
                <p className="text-xs text-muted-foreground">
                  填写“最近N天”或下方自定义开始/结束时间，留空则不限制该项。
                </p>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <div className="space-y-2">
                  <Label>开始时间（可选）</Label>
                  <Input
                    type="datetime-local"
                    value={startTime}
                    onChange={(e) => setStartTime(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label>结束时间（可选）</Label>
                  <Input
                    type="datetime-local"
                    value={endTime}
                    onChange={(e) => setEndTime(e.target.value)}
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label>每页数量</Label>
                <Input
                  type="number"
                  min="1"
                  max="100"
                  value={perPage}
                  onChange={(e) => {
                    const v = e.target.value;
                    if (v === '') setPerPage('');
                    else {
                      const n = parseInt(v);
                      if (!Number.isNaN(n)) setPerPage(n);
                    }
                  }}
                  onBlur={(e) => {
                    if (e.target.value === '' || parseInt(e.target.value) < 1) {
                      setPerPage(defaultPerPage);
                    }
                  }}
                />
                <p className="text-xs text-muted-foreground">
                  用于时间区间采集的每页数量，默认 {defaultPerPage}。
                </p>
              </div>
            </div>
          ) : (
            <div className="text-xs text-muted-foreground space-y-1">
              <p>• 直接点击“确认开始”将从最新话题向后增量抓取。</p>
              <p>• 如需限定时间范围，请切换到“按时间区间”。</p>
            </div>
          )}
        </div>

        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={submitting}
          >
            取消
          </Button>
          <Button type="button" onClick={handleConfirm} disabled={isConfirmDisabled}>
            {submitting ? '创建任务中...' : '确认开始'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}