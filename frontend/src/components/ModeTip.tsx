'use client';

import React from 'react';
import { HelpCircle } from 'lucide-react';
import { cn } from '@/lib/utils';

interface ModeTipProps {
  children: React.ReactNode;
  className?: string;
  panelClassName?: string;
  ariaLabel?: string;
}

export default function ModeTip({
  children,
  className,
  panelClassName,
  ariaLabel = '查看模式说明',
}: ModeTipProps) {
  return (
    <span
      className={cn('absolute right-2 top-2 z-20 inline-flex', className)}
      onClick={(event) => event.stopPropagation()}
    >
      <span
        role="button"
        tabIndex={0}
        aria-label={ariaLabel}
        title={typeof children === 'string' ? children : ariaLabel}
        className="group relative inline-flex h-5 w-5 items-center justify-center rounded-full border border-gray-200 bg-white text-gray-400 shadow-sm transition-colors hover:border-blue-200 hover:text-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-200"
      >
        <HelpCircle className="h-3.5 w-3.5" />
        <span
          className={cn(
            'pointer-events-none invisible absolute right-0 top-6 z-50 w-64 rounded-md border border-gray-200 bg-white p-3 text-left text-xs leading-relaxed text-gray-700 opacity-0 shadow-lg transition-opacity group-hover:visible group-hover:opacity-100 group-focus:visible group-focus:opacity-100',
            panelClassName,
          )}
        >
          {children}
        </span>
      </span>
    </span>
  );
}
