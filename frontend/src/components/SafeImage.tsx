'use client';

import { useState } from 'react';

interface SafeImageProps {
  src?: string;
  alt: string;
  className?: string;
  fallbackClassName?: string;
  fallbackText?: string;
  fallbackGradient?: string;
}

export default function SafeImage({ 
  src, 
  alt, 
  className = '', 
  fallbackClassName = '',
  fallbackText,
  fallbackGradient = 'from-blue-400 to-purple-500'
}: SafeImageProps) {
  const [imageError, setImageError] = useState(false);
  const [imageLoading, setImageLoading] = useState(true);

  // 如果没有图片URL或图片加载失败，显示渐变背景
  if (!src || imageError) {
    return (
      <div 
        className={`bg-gradient-to-br ${fallbackGradient} flex items-center justify-center ${fallbackClassName || className}`}
      >
        {fallbackText && (
          <span className="text-white font-medium text-sm opacity-80">
            {fallbackText}
          </span>
        )}
      </div>
    );
  }

  return (
    <div className={`relative ${className}`}>
      {imageLoading && (
        <div 
          className={`absolute inset-0 bg-gradient-to-br ${fallbackGradient} animate-pulse flex items-center justify-center`}
        >
          <div className="w-6 h-6 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
        </div>
      )}
      <img
        src={src}
        alt={alt}
        className={`${className} ${imageLoading ? 'opacity-0' : 'opacity-100'} transition-opacity duration-300`}
        onLoad={() => setImageLoading(false)}
        onError={() => {
          setImageError(true);
          setImageLoading(false);
        }}
        referrerPolicy="no-referrer"
        crossOrigin="anonymous"
      />
    </div>
  );
}
