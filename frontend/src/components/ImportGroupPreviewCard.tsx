'use client';

import { Crown, FileText, MessageSquare, Users } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { ExportManifestGroup } from '@/lib/api';
import SafeImage from './SafeImage';

interface ImportGroupPreviewCardProps {
  group: ExportManifestGroup;
  conflicted?: boolean;
}

function getGradientByType(type?: string) {
  switch (type) {
    case 'private':
      return 'from-purple-400 to-pink-500';
    case 'public':
      return 'from-blue-400 to-cyan-500';
    default:
      return 'from-gray-400 to-gray-600';
  }
}

function formatBytes(bytes?: number) {
  if (!bytes || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(value >= 10 || index === 0 ? 0 : 1)} ${units[index]}`;
}

function getCount(group: ExportManifestGroup, nestedKey: 'members' | 'topics' | 'files') {
  const statistics = group.statistics || {};
  if (nestedKey === 'members') {
    return statistics.members?.count ?? statistics.members_count ?? group.members_count ?? 0;
  }
  if (nestedKey === 'topics') {
    return statistics.topics?.topics_count ?? statistics.topics_count ?? group.topics_count ?? 0;
  }
  return statistics.files?.count ?? statistics.files_count ?? group.files_count ?? 0;
}

export default function ImportGroupPreviewCard({ group, conflicted = false }: ImportGroupPreviewCardProps) {
  const coverSrc = group.cover_image_data_url || group.cover_url || group.background_url;
  const ownerName = group.owner?.name || group.owner?.alias;
  const topicsCount = getCount(group, 'topics');
  const membersCount = getCount(group, 'members');
  const filesCount = getCount(group, 'files');

  return (
    <div className={`bg-card border rounded-lg overflow-hidden w-full flex items-stretch ${conflicted ? 'border-destructive/50' : 'border-border'}`}>
      <div className="w-20 sm:w-24 shrink-0 border-r border-border">
        <SafeImage
          src={coverSrc}
          alt={group.name}
          className="w-full h-full min-h-20 sm:min-h-24 object-cover"
          fallbackClassName="w-full h-full min-h-20 sm:min-h-24 bg-gradient-to-br"
          fallbackText={group.name.slice(0, 2)}
          fallbackGradient={getGradientByType(group.type)}
        />
      </div>
      <div className="p-2.5 min-w-0 flex-1 space-y-1.5">
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <h3 className="text-sm font-semibold text-foreground line-clamp-1">{group.name}</h3>
            <div className="text-[11px] text-muted-foreground">ID: {group.group_id}</div>
          </div>
          {conflicted && <Badge variant="destructive" className="shrink-0 h-5 px-1.5">冲突</Badge>}
        </div>

        <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">
          <div className="flex items-center gap-1 min-w-0">
            {ownerName && (
              <>
                <Crown className="h-3 w-3 shrink-0" />
                <span className="truncate">{ownerName}</span>
              </>
            )}
          </div>
          <div className="flex items-center gap-1 shrink-0">
            <span>{formatBytes(group.size_bytes)}</span>
          </div>
        </div>

        <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
          <div className="rounded-md bg-muted/50 px-1.5 py-0.5 flex items-center gap-1" title="成员数">
            <Users className="h-3 w-3" />
            <span>成员 {membersCount}</span>
          </div>
          <div className="rounded-md bg-muted/50 px-1.5 py-0.5 flex items-center gap-1" title="话题数">
            <MessageSquare className="h-3 w-3" />
            <span>话题 {topicsCount}</span>
          </div>
          <div className="rounded-md bg-muted/50 px-1.5 py-0.5 flex items-center gap-1" title="文件数">
            <FileText className="h-3 w-3" />
            <span>文件 {filesCount}</span>
          </div>
        </div>

        <div className="flex items-center gap-1 flex-wrap">
          <Badge variant="outline" className="text-[11px] px-1.5 py-0 h-4 font-normal">
            导入包
          </Badge>
          {group.type && group.type !== 'local' && (
            <Badge variant="outline" className="text-[11px] px-1.5 py-0 h-4 font-normal text-muted-foreground">
              {group.type === 'pay' ? '付费' : group.type === 'private' ? '私密' : '公开'}
            </Badge>
          )}
        </div>
      </div>
    </div>
  );
}
