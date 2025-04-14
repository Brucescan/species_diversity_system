
class GetCurrentUserView(APIView):
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        try:
            serializer = UserSerializer(request.user)
            # 添加调试信息
            print(f"Current user: {request.user}")
            print(f"Serialized data: {serializer.data}")
            return Response(serializer.data)
        except Exception as e:
            # 打印异常信息
            print(f"Exception in GetCurrentUserView: {str(e)}")
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
