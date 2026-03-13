import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Atlas STF · Painel Analítico",
  description: "Painel para entender períodos, casos e pontos de atenção do Atlas STF com linguagem mais clara e leitura guiada.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="pt-BR">
      <body className="font-sans antialiased">
        {children}
      </body>
    </html>
  );
}
