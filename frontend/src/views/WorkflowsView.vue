<script setup lang="ts">
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
</script>

<template>
  <section class="grid gap-6">
    <Card>
      <CardHeader>
        <CardTitle>Workflow Module</CardTitle>
        <CardDescription>
          SQL workflow list, reviews, and execution actions.
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-4">
        <Input placeholder="Search workflow title..." />
        <div class="flex flex-wrap gap-2">
          <Badge variant="secondary">Workflow list</Badge>
          <Badge variant="secondary">Submit workflow</Badge>
          <Badge variant="secondary">Review workflow</Badge>
          <Badge variant="secondary">Execute workflow</Badge>
        </div>
      </CardContent>
    </Card>
  </section>
</template>
