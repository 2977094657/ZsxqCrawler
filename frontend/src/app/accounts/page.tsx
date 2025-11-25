import Link from 'next/link';
import AccountPanel from '@/components/AccountPanel';
import { Button } from '@/components/ui/button';

export default function AccountsPage() {
  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto p-4">
        <div className="mb-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold mb-1">账号管理</h1>
              <p className="text-sm text-muted-foreground">添加、删除、设为默认，查看账号信息</p>
            </div>
            <Link href="/">
              <Button variant="outline" className="flex items-center gap-2">← 返回首页</Button>
            </Link>
          </div>
        </div>
        <AccountPanel />
      </div>
    </div>
  );
}