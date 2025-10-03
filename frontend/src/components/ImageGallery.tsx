'use client';

import React, { useState } from 'react';
import Lightbox from 'yet-another-react-lightbox';
import Zoom from 'yet-another-react-lightbox/plugins/zoom';
import Fullscreen from 'yet-another-react-lightbox/plugins/fullscreen';
import { apiClient } from '@/lib/api';

interface ImageData {
  image_id: string;
  original?: { url: string };
  large?: { url: string };
  thumbnail?: { url: string };
}

interface ImageGalleryProps {
  images: ImageData[];
  className?: string;
  size?: 'small' | 'medium' | 'large';
  groupId?: string;
}

const ImageGallery: React.FC<ImageGalleryProps> = ({ images, className = '', size = 'medium', groupId }) => {
  const [lightboxOpen, setLightboxOpen] = useState(false);
  const [currentImageIndex, setCurrentImageIndex] = useState(0);

  // 如果没有图片，不渲染组件
  if (!images || images.length === 0) {
    return null;
  }

  // 准备 lightbox 的图片数据
  const lightboxSlides = images.map((image) => ({
    src: apiClient.getProxyImageUrl(image.original?.url || image.large?.url || image.thumbnail?.url || '', groupId),
    alt: '话题图片',
  }));

  // 处理缩略图点击
  const handleThumbnailClick = (index: number) => {
    setCurrentImageIndex(index);
    setLightboxOpen(true);
  };

  // 获取缩略图URL，优先使用thumbnail，然后large，最后original
  const getThumbnailUrl = (image: ImageData) => {
    return apiClient.getProxyImageUrl(
      image.thumbnail?.url || image.large?.url || image.original?.url || '',
      groupId
    );
  };

  // 获取预览图URL，优先使用original，然后large
  const getPreviewUrl = (image: ImageData) => {
    return apiClient.getProxyImageUrl(
      image.original?.url || image.large?.url || image.thumbnail?.url || '',
      groupId
    );
  };

  // 根据size属性获取对应的样式类（固定缩略图盒子尺寸，避免加载时宽度抖动）
  const getSizeClasses = () => {
    switch (size) {
      case 'small':
        return 'w-16 h-16';
      case 'large':
        return 'w-40 h-40';
      case 'medium':
      default:
        return 'w-32 h-32';
    }
  };

  return (
    <div className={`space-y-2 w-full max-w-full overflow-hidden ${className}`}>
      {/* 缩略图网格 */}
      <div className="flex gap-2 overflow-x-auto pb-2 w-full">
        {images.map((image, index) => (
          <div key={image.image_id} className={`relative flex-shrink-0 ${getSizeClasses()}`}>
            <img
              src={getThumbnailUrl(image)}
              alt={`话题图片 ${index + 1}`}
              className={`w-full h-full rounded-lg border border-gray-200 cursor-pointer hover:opacity-90 transition-opacity object-cover`}
              loading="lazy"
              decoding="async"
              onClick={() => handleThumbnailClick(index)}
              onError={(e) => {
                // 图片加载失败时的处理
                const target = e.currentTarget;
                if (target.src.includes('thumbnail') && image.large?.url) {
                  target.src = apiClient.getProxyImageUrl(image.large.url, groupId);
                } else if (target.src.includes('large') && image.original?.url) {
                  target.src = apiClient.getProxyImageUrl(image.original.url, groupId);
                } else {
                  // 所有尺寸都失败时，显示占位符
                  target.style.display = 'none';
                }
              }}
            />
            
            {/* 多图时显示图片数量标识 */}
            {images.length > 1 && (
              <div className="absolute top-1 right-1 bg-black bg-opacity-60 text-white text-xs px-1.5 py-0.5 rounded">
                {index + 1}/{images.length}
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Lightbox 组件 */}
      <Lightbox
        open={lightboxOpen}
        close={() => setLightboxOpen(false)}
        slides={lightboxSlides}
        index={currentImageIndex}
        // 启用插件
        plugins={[Zoom, Fullscreen]}
        // 配置选项
        carousel={{
          finite: images.length <= 1, // 单图时不循环
        }}
        // 缩放配置
        zoom={{
          maxZoomPixelRatio: 3, // 最大缩放比例
          zoomInMultiplier: 2, // 缩放倍数
          doubleTapDelay: 300, // 双击延迟
          doubleClickDelay: 300, // 双击延迟
          doubleClickMaxStops: 2, // 双击最大停止次数
          keyboardMoveDistance: 300, // 键盘移动距离（最大化）
          wheelZoomDistanceFactor: 10, // 滚轮缩放距离因子（最小化以提高灵敏度）
          pinchZoomDistanceFactor: 10, // 捏合缩放距离因子（最小化以提高灵敏度）
          scrollToZoom: true, // 滚轮缩放
        }}
        render={{
          buttonPrev: images.length <= 1 ? () => null : undefined,
          buttonNext: images.length <= 1 ? () => null : undefined,
        }}
        // 样式配置
        styles={{
          container: {
            backgroundColor: 'rgba(0, 0, 0, 0.9)',
            zIndex: 9999,
            position: 'fixed',
            top: 0,
            left: 0,
            width: '100vw',
            height: '100vh'
          },
        }}
        // 动画配置
        animation={{
          fade: 300,
          swipe: 500,
          zoom: 200, // 缩放动画时间，减少以提高响应速度
        }}
      />
    </div>
  );
};

export default ImageGallery;
