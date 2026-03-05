<script setup lang="ts">
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
</script>

<template>
  <section class="grid gap-6">
    <Card>
      <CardHeader>
        <CardTitle>SQL Query Center</CardTitle>
        <CardDescription>Search logs, review recent queries, and monitor execution outcomes.</CardDescription>
      </CardHeader>
      <CardContent class="space-y-4">
        <Input placeholder="Search by database, user, or SQL text" />
        <div class="flex flex-wrap gap-2">
          <Badge variant="secondary">Recent queries</Badge>
          <Badge variant="secondary">Favorites</Badge>
          <Badge variant="secondary">Audit queue</Badge>
        </div>
      </CardContent>
    </Card>
  </section>
</template>
